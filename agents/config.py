"""
Configuration – loaded from environment variables / .env file.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    # ── Azure AI Foundry (OpenAI Assistants API) ──────────────────────
    ai_agents_endpoint: str = field(
        default_factory=lambda: os.environ["AZURE_AI_AGENTS_ENDPOINT"]
    )
    ai_agents_key: str = field(
        default_factory=lambda: os.environ.get("AZURE_AI_AGENTS_KEY", "")
    )
    model_deployment: str = field(
        default_factory=lambda: os.environ.get("MODEL_DEPLOYMENT_NAME", "gpt-4o")
    )

    # ── Azure SQL (Entra-only authentication) ─────────────────────────
    sql_server: str = field(
        default_factory=lambda: os.environ["SQL_SERVER"]
    )
    sql_database: str = field(
        default_factory=lambda: os.environ.get(
            "SQL_DATABASE", "DataQualityWorkshop"
        )
    )
    sql_driver: str = field(
        default_factory=lambda: os.environ.get(
            "SQL_DRIVER", "{ODBC Driver 18 for SQL Server}"
        )
    )

    # ── Notification defaults ─────────────────────────────────────────
    teams_webhook_url: str = field(
        default_factory=lambda: os.environ.get("TEAMS_WEBHOOK_URL", "")
    )

    # ── Agent tunables ────────────────────────────────────────────────
    lookback_days: int = field(
        default_factory=lambda: int(os.environ.get("LOOKBACK_DAYS", "7"))
    )

    @property
    def fq_kpi_table(self) -> str:
        return "dbo.daily_kpi_log"

    @property
    def fq_findings_table(self) -> str:
        return "dbo.agent_findings"


settings = Settings()
