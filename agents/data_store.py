"""
Azure SQL data store — creates tables on agent startup.

Tables are created under ``dbo`` schema.  A **single persistent
connection** is reused for all queries during the agent's lifetime.

On startup the module:
1. Creates ``dbo.daily_kpi_log`` and ``dbo.agent_findings`` tables
2. Seeds 60 days × 5 KPIs of clean data

Call ``inject_anomalies()`` mid-demo to dirty the data.
Call ``reset_data()`` to wipe and re-seed clean data.
Call ``teardown()`` at shutdown to DROP the tables (optional).
"""

from __future__ import annotations

import math
import struct
from datetime import date, timedelta

from azure.identity import DefaultAzureCredential

import pyodbc

from agents.config import settings

# ─────────────────────────────────────────────────────────────────────────────
# Singleton connection
# ─────────────────────────────────────────────────────────────────────────────

_SQL_TOKEN_SCOPE = "https://database.windows.net/.default"
_SQL_COPT_SS_ACCESS_TOKEN = 1256
_credential = DefaultAzureCredential()
_conn: pyodbc.Connection | None = None

_KPI = "dbo.daily_kpi_log"
_FINDINGS = "dbo.agent_findings"


def _open_connection() -> pyodbc.Connection:
    token = _credential.get_token(_SQL_TOKEN_SCOPE)
    token_bytes = token.token.encode("UTF-16-LE")
    token_struct = struct.pack(
        f"<I{len(token_bytes)}s", len(token_bytes), token_bytes
    )
    conn_str = (
        f"Driver={settings.sql_driver};"
        f"Server={settings.sql_server};"
        f"Database={settings.sql_database};"
    )
    return pyodbc.connect(
        conn_str,
        attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
    )


def get_connection() -> pyodbc.Connection:
    """Return the shared Azure SQL connection (lazy init + seed on first call)."""
    global _conn
    if _conn is None:
        print("🔌  Connecting to Azure SQL…")
        _conn = _open_connection()
        _create_tables()
        _seed_clean_data()
    return _conn


def close_connection() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


# ─────────────────────────────────────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────────────────────────────────────

def _create_tables() -> None:
    assert _conn is not None
    cur = _conn.cursor()

    # KPI log table
    cur.execute(f"""
        IF OBJECT_ID('{_KPI}', 'U') IS NOT NULL DROP TABLE {_KPI};
    """)
    cur.execute(f"""
        CREATE TABLE {_KPI} (
            Id                    INT IDENTITY(1,1) PRIMARY KEY,
            RunDate               DATE           NOT NULL,
            PipelineName          VARCHAR(100)   NOT NULL,
            PipelineStatus        VARCHAR(20)    NOT NULL,
            DurationSeconds       INT            NULL,
            AvgDurationSeconds    INT            NULL,
            RowsRead              INT            NULL,
            RowsWritten           INT            NULL,
            ErrorMessage          VARCHAR(500)   NULL,
            KPIName               VARCHAR(50)    NOT NULL,
            KPIValue              DECIMAL(18,2)  NULL,
            PreviousDayValue      DECIMAL(18,2)  NULL,
            DayOverDayChangePct   DECIMAL(8,2)   NULL,
            ExpectedMin           DECIMAL(18,2)  NULL,
            ExpectedMax           DECIMAL(18,2)  NULL,
            AggregationType       VARCHAR(10)    NOT NULL
        );
    """)

    # Agent findings table (separate, not UPDATE on same row)
    cur.execute(f"""
        IF OBJECT_ID('{_FINDINGS}', 'U') IS NOT NULL DROP TABLE {_FINDINGS};
    """)
    cur.execute(f"""
        CREATE TABLE {_FINDINGS} (
            Id              INT IDENTITY(1,1) PRIMARY KEY,
            SourceRowId     INT            NOT NULL,
            RunDate         DATE           NULL,
            PipelineName    VARCHAR(100)   NULL,
            KPIName         VARCHAR(50)    NULL,
            AggregationType VARCHAR(10)    NULL,
            AgentName       VARCHAR(50)    NULL,
            AnomalyType     VARCHAR(50)    NULL,
            Severity        VARCHAR(20)    NULL,
            Description     VARCHAR(2000)  NULL,
            Hypothesis      VARCHAR(2000)  NULL,
            NotifiedTeam    VARCHAR(100)   NULL,
            NotifiedUsers   VARCHAR(200)   NULL,
            NotificationMsg VARCHAR(2000)  NULL,
            DetectedAt      DATETIME2      DEFAULT GETDATE()
        );
    """)
    _conn.commit()
    print(f"📋  Created {_KPI} and {_FINDINGS}")


# ─────────────────────────────────────────────────────────────────────────────
# Seed 60 days × 5 KPIs  (deterministic)
# ─────────────────────────────────────────────────────────────────────────────

PIPELINES = {
    "Revenue":     ("PL_Revenue_Daily",  "Daily",  80000.0, 150000.0, 180),
    "Revenue_YTD": ("PL_Revenue_Daily",  "YTD",    None,    None,     180),
    "Orders":      ("PL_Orders_Daily",   "Daily",  800.0,   1500.0,   120),
    "Returns":     ("PL_Returns_Daily",  "Daily",  20.0,    80.0,     60),
    "AOV":         ("PL_Revenue_Daily",  "Daily",  95.0,    115.0,    180),
}

DAYS = 60


def _seed_clean_data() -> None:
    assert _conn is not None
    today = date.today()
    cumulative_revenue = 0.0
    prev: dict[str, float] = {}

    cur = _conn.cursor()

    for day_offset in range(DAYS, 0, -1):
        jitter = math.sin(day_offset * 2.7) * 0.08
        weekday = day_offset % 7
        wk_factor = 0.65 if weekday >= 5 else 1.0

        for kpi, (pipe, agg, exp_min, exp_max, avg_dur) in PIPELINES.items():
            if kpi == "Revenue":
                base_val = round(115000 * wk_factor + 115000 * jitter, 2)
            elif kpi == "Orders":
                base_val = round(1150 * wk_factor + 1150 * jitter * 0.5, 0)
            elif kpi == "Returns":
                base_val = round(50 + 30 * jitter, 0)
            elif kpi == "AOV":
                base_val = round(105 + 10 * jitter, 2)
            elif kpi == "Revenue_YTD":
                cumulative_revenue += prev.get("Revenue", 115000.0)
                base_val = round(cumulative_revenue, 2)
            else:
                base_val = 0

            prev_val = prev.get(kpi)
            d_o_d = None
            if prev_val and prev_val != 0:
                d_o_d = round((base_val - prev_val) / prev_val * 100, 2)
            prev[kpi] = base_val

            dur = max(30, int(avg_dur + avg_dur * jitter * 0.3))
            rows_base = int(base_val) if kpi in ("Orders", "Returns") else int(base_val / 10)
            rows_read = rows_base + int(rows_base * jitter * 0.1)
            rows_written = rows_read

            cur.execute(
                f"""
                INSERT INTO {_KPI}
                    (RunDate, PipelineName, PipelineStatus, DurationSeconds,
                     AvgDurationSeconds, RowsRead, RowsWritten, ErrorMessage,
                     KPIName, KPIValue, PreviousDayValue, DayOverDayChangePct,
                     ExpectedMin, ExpectedMax, AggregationType)
                VALUES (?, ?, 'Succeeded', ?, ?, ?, ?, NULL,
                        ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    today - timedelta(days=day_offset),
                    pipe, dur, avg_dur, rows_read, rows_written,
                    kpi, base_val, prev_val, d_o_d,
                    exp_min, exp_max, agg,
                ],
            )

    _conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {_KPI}")
    count = cur.fetchone()[0]
    cur.close()
    print(f"🌱  Seeded {count} clean rows into {_KPI}")


# ─────────────────────────────────────────────────────────────────────────────
# Anomaly injection / reset
# ─────────────────────────────────────────────────────────────────────────────

def inject_anomalies() -> int:
    """Inject 5 anomalies. Returns total dirty rows."""
    conn = get_connection()
    cur = conn.cursor()

    anomalies = [
        # 1  Today: PL_Returns failed + NULL KPI
        f"""UPDATE {_KPI}
            SET PipelineStatus = 'Failed',
                KPIValue = NULL, RowsRead = 0, RowsWritten = 0,
                ErrorMessage = 'Connection timeout after 300 s - source API unreachable'
            WHERE RunDate = CAST(GETDATE() AS DATE)
                  AND PipelineName = 'PL_Returns_Daily'""",
        # 2  Day-3: Revenue & Orders zero rows
        f"""UPDATE {_KPI}
            SET RowsRead = 0, RowsWritten = 0, KPIValue = 0
            WHERE RunDate = DATEADD(DAY, -3, CAST(GETDATE() AS DATE))
                  AND PipelineName IN ('PL_Revenue_Daily', 'PL_Orders_Daily')
                  AND AggregationType = 'Daily'""",
        # 3  Day-7: Revenue_YTD dropped -50k
        f"""UPDATE {_KPI}
            SET KPIValue = PreviousDayValue - 50000,
                DayOverDayChangePct = -2.50
            WHERE RunDate = DATEADD(DAY, -7, CAST(GETDATE() AS DATE))
                  AND KPIName = 'Revenue_YTD'""",
        # 4  Day-14: Duration spike 900s
        f"""UPDATE {_KPI}
            SET DurationSeconds = 900
            WHERE RunDate = DATEADD(DAY, -14, CAST(GETDATE() AS DATE))
                  AND PipelineName = 'PL_Revenue_Daily'
                  AND AggregationType = 'Daily'""",
        # 5  Day-30: AOV range breach $250
        f"""UPDATE {_KPI}
            SET KPIValue = 250.00, DayOverDayChangePct = 138.10
            WHERE RunDate = DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
                  AND KPIName = 'AOV'""",
    ]

    for sql in anomalies:
        cur.execute(sql)
    conn.commit()
    cur.close()

    # Count dirty rows
    cur2 = conn.cursor()
    cur2.execute(f"""
        SELECT COUNT(*) FROM {_KPI}
        WHERE PipelineStatus = 'Failed'
           OR (KPIValue = 0 AND ExpectedMin > 0)
           OR (AggregationType = 'YTD' AND KPIValue < ISNULL(PreviousDayValue, 0))
           OR (AvgDurationSeconds > 0 AND DurationSeconds > AvgDurationSeconds * 3)
           OR (ExpectedMax IS NOT NULL AND KPIValue > ExpectedMax * 2)
           OR KPIValue IS NULL
    """)
    total = cur2.fetchone()[0]
    cur2.close()
    return total


def reset_data() -> None:
    """Truncate both tables and re-seed clean data."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {_KPI}")
    cur.execute(f"TRUNCATE TABLE {_FINDINGS}")
    conn.commit()
    cur.close()
    _seed_clean_data()
    print("🔄  Data reset to clean state")


def teardown() -> None:
    """Drop tables and close connection (end of demo)."""
    if _conn is not None:
        cur = _conn.cursor()
        cur.execute(f"IF OBJECT_ID('{_KPI}', 'U') IS NOT NULL DROP TABLE {_KPI}")
        cur.execute(f"IF OBJECT_ID('{_FINDINGS}', 'U') IS NOT NULL DROP TABLE {_FINDINGS}")
        _conn.commit()
        cur.close()
        print(f"🧹  Dropped {_KPI} and {_FINDINGS}")
    close_connection()


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard helpers
# ─────────────────────────────────────────────────────────────────────────────

def count_kpi_rows() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {_KPI}")
    n = cur.fetchone()[0]
    cur.close()
    return n


def count_dirty_rows() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) FROM {_KPI}
        WHERE PipelineStatus = 'Failed'
           OR (KPIValue = 0 AND ExpectedMin > 0)
           OR (AggregationType = 'YTD' AND KPIValue < ISNULL(PreviousDayValue, 0))
           OR (AvgDurationSeconds > 0 AND DurationSeconds > AvgDurationSeconds * 3)
           OR (ExpectedMax IS NOT NULL AND KPIValue > ExpectedMax * 2)
           OR KPIValue IS NULL
    """)
    n = cur.fetchone()[0]
    cur.close()
    return n


def count_findings() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {_FINDINGS}")
    n = cur.fetchone()[0]
    cur.close()
    return n


def get_all_findings() -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT Id, SourceRowId, RunDate, PipelineName,
               KPIName, AggregationType,
               AgentName, AnomalyType, Severity,
               Description, Hypothesis,
               NotifiedTeam, NotifiedUsers, NotificationMsg,
               DetectedAt
        FROM {_FINDINGS}
        ORDER BY
            CASE Severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH' THEN 2
                WHEN 'WARNING' THEN 3
                WHEN 'INFO' THEN 4
                ELSE 5
            END,
            RunDate DESC
    """)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    cur.close()
    from datetime import datetime, date as _date
    result = []
    for row in rows:
        d = {}
        for col, val in zip(cols, row):
            if isinstance(val, (datetime, _date)):
                d[col] = val.isoformat()
            else:
                d[col] = val
        result.append(d)
    return result


def clear_findings() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {_FINDINGS}")
    conn.commit()
    cur.close()
