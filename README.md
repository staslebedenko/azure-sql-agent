# Data Quality AI Agents — Azure SQL Edition

Two **Azure AI Foundry** agents that monitor data quality on an **Azure SQL Database**.
Everything is self-contained — tables are created, seeded, and torn down automatically.

| Agent | What it does |
|-------|-------------|
| **Platform Monitor** | Detects pipeline failures, duration spikes, zero-row loads |
| **Report Validator** | Validates KPI temporal logic (Daily vs YTD), range breaches |

---

## Quick Start

```bash
# 1. Login to Azure (used for both AI Foundry and SQL auth)
az login

# 2. Create venv & install
python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows
# source .venv/bin/activate          # macOS / Linux
pip install -r requirements.txt

# 3. Configure
cp .env.example .env                 # fill in your values

# 4. Run demo (creates tables → seeds data → scans → tears down)
python run_demo.py --once            # single clean scan
python run_demo.py --inject --once   # inject anomalies + scan
python run_demo.py --inject          # continuous loop with anomalies
python run_demo.py                   # continuous loop, clean data
```

### Key `.env` values

| Variable | Where to find it |
|----------|-----------------|
| `AZURE_AI_AGENTS_ENDPOINT` | AI Foundry → Project → Overview |
| `AZURE_AI_AGENTS_KEY` | AI Foundry → Project → Keys |
| `SQL_SERVER` | e.g. `myserver.database.windows.net` |
| `SQL_DATABASE` | Database name (default: `DataQualityWorkshop`) |

---

## What Happens at Runtime

1. Connects to Azure SQL (Entra ID token via `az login`)
2. Creates `dbo.daily_kpi_log` + `dbo.agent_findings` tables
3. Seeds 60 days × 5 KPIs of clean pipeline data (300 rows)
4. Optionally injects 5 anomalies (`--inject` flag)
5. Both AI agents scan the data, flag issues, send notifications
6. Tables are dropped on exit (use `--no-teardown` to keep them)

---

## CLI Options

```bash
python run_demo.py --interval 15 --days 30   # custom timing
python run_demo.py --inject --once            # inject + single scan
python run_demo.py --no-teardown              # keep tables after exit
python run_monitor.py --days 14               # run Agent 1 alone
python run_validator.py --days 30             # run Agent 2 alone
python run_validator.py --prompt "Check only YTD KPIs"
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Azure OpenAI  (Assistants API)                                      │
│  ┌──────────────────┐   ┌──────────────────┐                        │
│  │ Platform Monitor │   │ Report Validator  │                        │
│  │  (Agent 1)       │   │  (Agent 2)       │                        │
│  └────────┬─────────┘   └────────┬─────────┘                        │
│           │ function calls        │ function calls                   │
└───────────┼──────────────────────┼───────────────────────────────────┘
            ▼                      ▼
    ┌──────────────────────────────────────┐
    │  db_tools.py  (11 tool functions)    │
    │  ← shared pyodbc connection          │
    └──────────────────┬───────────────────┘
                       ▼
    ┌──────────────────────────────────────┐
    │  Azure SQL Database                  │
    │    dbo.daily_kpi_log  (pipeline KPIs)│
    │    dbo.agent_findings (agent output) │
    └──────────────────────────────────────┘
```

---

## Authentication

- **Azure SQL**: Entra ID token via `DefaultAzureCredential` (`az login`)
- **Azure OpenAI**: API key from `.env` (`AZURE_AI_AGENTS_KEY`)
- No SQL passwords — Entra-only authentication

---

## Project Structure

```
agents_project/
├── agents/
│   ├── config.py              # Settings from .env
│   ├── data_store.py          # Singleton connection, DDL, seed, inject, teardown
│   ├── db_tools.py            # 11 agent tool functions (shared connection)
│   ├── _openai_helpers.py     # Schema generation + tool dispatch loop
│   ├── platform_monitor.py    # Agent 1 — pipeline anomaly detection
│   └── report_validator.py    # Agent 2 — KPI temporal validation
├── database/
│   ├── seed_data.sql          # Standalone SQL seed (optional, backup)
│   └── inject_anomalies.sql   # Standalone SQL inject (optional, backup)
├── run_demo.py                # Main demo loop with dashboard
├── run_monitor.py             # One-shot Agent 1
├── run_validator.py           # One-shot Agent 2
├── requirements.txt
├── .env.example
└── README.md
```
