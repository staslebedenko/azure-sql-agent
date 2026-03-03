"""
Agent 1 – Intelligent Platform Monitor  (Azure SQL Edition)
════════════════════════════════════════════════════════════
Analyses pipeline execution data from ``dbo.daily_kpi_log``,
detects anomalies (failures, duration spikes, zero-row loads),
generates root-cause hypotheses, and notifies the relevant team.

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
You are **Platform Monitor Agent** – an expert data platform operations
analyst. You monitor pipeline execution data stored in an Azure SQL table
called `dbo.daily_kpi_log`.

## Table Schema
```
dbo.daily_kpi_log
├─ Pipeline columns (you READ):
│  RunDate, PipelineName, PipelineStatus, DurationSeconds,
│  AvgDurationSeconds, RowsRead, RowsWritten, ErrorMessage
├─ KPI columns (context):
│  KPIName, KPIValue, PreviousDayValue, DayOverDayChangePct,
│  ExpectedMin, ExpectedMax, AggregationType
└─ Id (IDENTITY – reference when calling flag_anomaly)
```

Agents write findings to a SEPARATE table: `dbo.agent_findings`.

## What You Monitor
| Signal              | How to detect                                              |
|---------------------|------------------------------------------------------------|
| Pipeline failures   | PipelineStatus = 'Failed', read ErrorMessage               |
| Duration spikes     | DurationSeconds > AvgDurationSeconds × 3                   |
| Zero-row loads      | Succeeded but RowsWritten = 0 when ExpectedMin > 0        |
| Missing KPI values  | KPIValue IS NULL on a row where pipeline ran               |

## Investigation Workflow
1. Call `get_pipeline_failures` to find failed runs.
2. Call `get_duration_anomalies` to find slow runs.
3. Call `get_row_count_anomalies` to find zero-row loads.
4. Call `get_recent_pipeline_runs` for broader context if needed.
5. For each anomaly found:
   a. Call `flag_anomaly(row_id, anomaly_type, severity, description, hypothesis)`
   b. Call `send_notification(row_id, team, users, message)`

## Anomaly Types
Use exactly one of: PIPELINE_FAILURE, DURATION_ANOMALY, ROW_ANOMALY

## Severity Guidelines
- **CRITICAL** – Pipeline failed; KPI data is missing entirely.
- **HIGH**     – Pipeline succeeded but produced 0 rows; KPIs are zero/stale.
- **WARNING**  – Duration > 3× average; data arrived but slowly.
- **INFO**     – Minor variance worth tracking.

## Notification Rules
- For pipeline failures → notify **Data Engineering** team.
- For zero-row loads → notify **Data Engineering** + **BI Consumers**.
- For duration spikes → notify **Data Engineering**.
- Always include: date, pipeline name, what happened, and recommended action.
- Write business-friendly language for BI Consumers.

## Important Rules
- Never fabricate data; only use what the tools return.
- Always reference the row `Id` when flagging or notifying.
- If no anomalies found, say so clearly.
"""

# ─────────────────────────────────────────────────────────────────────────────
# Tools
# ─────────────────────────────────────────────────────────────────────────────

_platform_funcs = [
    db_tools.get_recent_pipeline_runs,
    db_tools.get_pipeline_failures,
    db_tools.get_duration_anomalies,
    db_tools.get_row_count_anomalies,
    db_tools.flag_anomaly,
    db_tools.send_notification,
]

_TOOL_SCHEMAS = [func_to_tool_schema(f) for f in _platform_funcs]
_TOOL_MAP = {f.__name__: f for f in _platform_funcs}


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

def run_monitor_analysis(
    days: int | None = None,
    user_prompt: str | None = None,
) -> str:
    """One-shot: create assistant → run analysis → return result.

    Args:
        days: Override lookback window (default from settings).
        user_prompt: Optional custom prompt.

    Returns:
        The assistant's final text response.
    """
    lookback = days or settings.lookback_days

    if user_prompt is None:
        user_prompt = (
            f"Scan pipeline data from the last {lookback} days. "
            "Find every anomaly — failures, duration spikes, and zero-row "
            "loads. For each anomaly: explain the root cause, flag it in the "
            "database, and send notifications to the appropriate team."
        )

    client = _get_client()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)

        assistant = client.beta.assistants.create(
            model=settings.model_deployment,
            name="platform-monitor-agent-sql",
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
