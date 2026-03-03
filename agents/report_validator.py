"""
Agent 2 – Context-Aware Report Validator  (Azure SQL Edition)
══════════════════════════════════════════════════════════════
Performs day-over-day comparison of KPI report outputs stored in
Azure SQL, understands temporal KPI logic (Daily vs YTD), detects
unrealistic movements and range breaches, and learns expected
ranges from historical data.

Uses Azure OpenAI Assistants API with function-calling tools that
query Azure SQL via the shared ``pyodbc`` connection.
"""

from __future__ import annotations

import warnings

from openai import AzureOpenAI

from agents.config import settings
from agents import db_tools
from agents._openai_helpers import func_to_tool_schema, run_assistant_with_tools

# ─────────────────────────────────────────────────────────────────────────────
# System prompt
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are **Report Validator Agent** – an expert BI data-quality analyst who
understands the semantics of business KPIs across different time aggregations.
Your data lives in an Azure SQL table.

## Table Schema
```
dbo.daily_kpi_log
├─ Pipeline columns (context):
│  RunDate, PipelineName, PipelineStatus, DurationSeconds,
│  AvgDurationSeconds, RowsRead, RowsWritten, ErrorMessage
├─ KPI columns (you READ):
│  KPIName, KPIValue, PreviousDayValue, DayOverDayChangePct,
│  ExpectedMin, ExpectedMax, AggregationType
└─ Id (IDENTITY – reference when calling flag_anomaly)
```

Agents write findings to: `dbo.agent_findings`

## KPIs in the Dataset
| KPI          | Type   | Pipeline          | Range       | Notes                     |
|--------------|--------|-------------------|-------------|---------------------------|
| Revenue      | Daily  | PL_Revenue_Daily  | 80k–150k    | Lower on weekends         |
| Revenue_YTD  | YTD    | PL_Revenue_Daily  | cumulative  | Must NEVER decrease       |
| Orders       | Daily  | PL_Orders_Daily   | 800–1500    | Lower on weekends         |
| Returns      | Daily  | PL_Returns_Daily  | 20–80       | More volatile             |
| AOV          | Daily  | PL_Revenue_Daily  | 95–115      | Revenue ÷ Orders          |

## KPI Temporal Logic (CRITICAL)
| Aggregation | Expected behaviour                                           |
|-------------|--------------------------------------------------------------|
| **Daily**   | Independent each day. Can fluctuate within expected range.   |
| **YTD**     | Monotonically increasing within a year (for additive KPIs    |
|             | like Revenue). A decrease is almost ALWAYS a data bug.       |

## Anomaly Detection Rules
1. **Out-of-range**: KPIValue outside [ExpectedMin, ExpectedMax].
2. **Excessive change**: |DayOverDayChangePct| > 50% for Daily KPIs.
3. **YTD regression**: Revenue_YTD decreased day-over-day → data error.
4. **Missing value**: KPIValue IS NULL (pipeline failed to produce data).
5. **Zero value**: KPIValue = 0 when ExpectedMin > 0 → empty load.

## Investigation Workflow
1. Call `get_kpi_summary` to see recent KPI values and changes.
2. Call `get_ytd_violations` to find YTD decreases.
3. Call `get_range_breaches` to find out-of-range values.
4. Call `get_kpi_statistics` or `get_kpi_history` for deeper context.
5. For each anomaly found:
   a. Call `flag_anomaly(row_id, anomaly_type, severity, description, hypothesis)`
   b. Call `send_notification(row_id, team, users, message)`

## Anomaly Types
Use exactly one of: KPI_DRIFT, RANGE_BREACH, YTD_LOGIC_VIOLATION, ROW_ANOMALY

## Severity Guidelines
- **CRITICAL** – YTD KPI decreased (logically impossible); KPI missing entirely.
- **HIGH**     – KPI = 0 when minimum > 0; value > 2× expected max.
- **WARNING**  – Value outside expected range but still plausible.
- **INFO**     – Marginal drift worth monitoring.

## Notification Rules
- For YTD violations → notify **Data Engineering** (bug) + **BI Consumers** (report trust).
- For range breaches → notify **Business Analysts**.
- For missing/zero KPIs → notify **Data Engineering** + **BI Consumers**.
- For BI Consumers: explain impact in business language
  ("The Executive Dashboard may show incorrect YTD revenue today").
- Always include: date, KPI name, actual vs expected value, and recommended action.

## Important Rules
- Never fabricate data; only use what the tools return.
- Always explain *why* a movement is unrealistic using temporal logic.
- Always reference the row `Id` when flagging or notifying.
- If all KPIs look healthy, say so clearly.
"""

# ─────────────────────────────────────────────────────────────────────────────
# Tools
# ─────────────────────────────────────────────────────────────────────────────

_validator_funcs = [
    db_tools.get_kpi_summary,
    db_tools.get_kpi_history,
    db_tools.get_kpi_statistics,
    db_tools.get_ytd_violations,
    db_tools.get_range_breaches,
    db_tools.flag_anomaly,
    db_tools.send_notification,
]

_TOOL_SCHEMAS = [func_to_tool_schema(f) for f in _validator_funcs]
_TOOL_MAP = {f.__name__: f for f in _validator_funcs}


# ─────────────────────────────────────────────────────────────────────────────
# Client factory
# ─────────────────────────────────────────────────────────────────────────────

def _get_client() -> AzureOpenAI:
    return AzureOpenAI(
        azure_endpoint=settings.ai_agents_endpoint,
        api_key=settings.ai_agents_key,
        api_version="2025-01-01-preview",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def run_report_validation(
    days: int | None = None,
    user_prompt: str | None = None,
) -> str:
    """One-shot: create assistant → validate reports → return result.

    Args:
        days: Lookback window (default from settings).
        user_prompt: Custom prompt override.

    Returns:
        The assistant's final text response.
    """
    lookback = days or settings.lookback_days

    if user_prompt is None:
        user_prompt = (
            f"Validate all KPI report outputs for the last {lookback} days. "
            "Check every KPI against its expected range and temporal logic. "
            "Find YTD violations, range breaches, missing values, and "
            "unrealistic movements. For each anomaly: explain why it's "
            "suspicious, flag it in the database, and send notifications "
            "to the appropriate teams."
        )

    client = _get_client()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)

        assistant = client.beta.assistants.create(
            model=settings.model_deployment,
            name="report-validator-agent-sql",
            instructions=SYSTEM_PROMPT,
            tools=_TOOL_SCHEMAS,
        )

        try:
            thread = client.beta.threads.create()
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=user_prompt,
            )

            return run_assistant_with_tools(
                client, assistant.id, thread.id, _TOOL_MAP,
            )
        finally:
            client.beta.assistants.delete(assistant.id)
