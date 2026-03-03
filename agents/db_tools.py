"""
Agent tool functions — query Azure SQL tables via shared connection.

All functions use the persistent connection from ``data_store`` which
creates the tables at startup and seeds them with clean data.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

from agents.config import settings
from agents.data_store import get_connection

# Table names
_KPI = settings.fq_kpi_table
_FINDINGS = settings.fq_findings_table


def _rows_to_dicts(cursor) -> list[dict]:
    if cursor.description is None:
        return []
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    result = []
    for row in rows:
        d = {}
        for col, val in zip(columns, row):
            if isinstance(val, (datetime, date)):
                d[col] = val.isoformat()
            elif isinstance(val, Decimal):
                d[col] = float(val)
            else:
                d[col] = val
        result.append(d)
    return result


# ═════════════════════════════════════════════════════════════════════════════
#  PLATFORM MONITOR TOOLS  (Agent 1)
# ═════════════════════════════════════════════════════════════════════════════

def get_recent_pipeline_runs(days: int = 7, status_filter: str = "all") -> str:
    """Retrieve recent pipeline execution data from the daily KPI log.

    Args:
        days: Number of days to look back. Default 7.
        status_filter: Filter by run status – 'all', 'Failed', 'Succeeded'.

    Returns:
        JSON array of rows with RunDate, PipelineName, PipelineStatus,
        DurationSeconds, AvgDurationSeconds, RowsRead, RowsWritten,
        ErrorMessage, KPIName, KPIValue.
    """
    query = f"""
        SELECT Id, RunDate, PipelineName, PipelineStatus,
               DurationSeconds, AvgDurationSeconds,
               RowsRead, RowsWritten, ErrorMessage,
               KPIName, KPIValue, AggregationType
        FROM   {_KPI}
        WHERE  RunDate >= DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
    """
    params: list = [days]
    if status_filter.lower() != "all":
        query += " AND PipelineStatus = ?"
        params.append(status_filter)
    query += " ORDER BY RunDate DESC, PipelineName, KPIName"

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


def get_pipeline_failures(days: int = 7) -> str:
    """Return all failed pipeline runs within the lookback window.

    Args:
        days: Number of days to look back. Default 7.

    Returns:
        JSON array of failed rows with full pipeline and error details.
    """
    query = f"""
        SELECT Id, RunDate, PipelineName, PipelineStatus,
               DurationSeconds, AvgDurationSeconds,
               RowsRead, RowsWritten, ErrorMessage,
               KPIName, KPIValue
        FROM   {_KPI}
        WHERE  PipelineStatus = 'Failed'
               AND RunDate >= DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
        ORDER BY RunDate DESC
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [days])
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


def get_duration_anomalies(threshold_multiplier: float = 3.0, days: int = 30) -> str:
    """Find pipeline runs where duration exceeded a multiple of the historical average.

    Args:
        threshold_multiplier: Flag runs where DurationSeconds > AvgDurationSeconds × this. Default 3.0.
        days: Lookback window. Default 30.

    Returns:
        JSON array of rows where duration exceeded the threshold.
    """
    query = f"""
        SELECT Id, RunDate, PipelineName, KPIName,
               DurationSeconds, AvgDurationSeconds,
               CAST(ROUND(CAST(DurationSeconds AS FLOAT)
                    / NULLIF(AvgDurationSeconds, 0), 1) AS FLOAT) AS DurationMultiplier
        FROM   {_KPI}
        WHERE  AvgDurationSeconds > 0
               AND DurationSeconds > AvgDurationSeconds * ?
               AND RunDate >= DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
        ORDER BY RunDate DESC
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [threshold_multiplier, days])
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


def get_row_count_anomalies(days: int = 7) -> str:
    """Find rows where RowsWritten is 0 despite a Succeeded status.

    Args:
        days: Lookback window. Default 7.

    Returns:
        JSON array of suspicious rows.
    """
    query = f"""
        SELECT Id, RunDate, PipelineName, PipelineStatus, KPIName,
               RowsRead, RowsWritten, KPIValue, ExpectedMin, ExpectedMax
        FROM   {_KPI}
        WHERE  RunDate >= DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
               AND (
                   (PipelineStatus = 'Succeeded' AND RowsWritten = 0 AND ExpectedMin > 0)
                   OR (RowsRead > 0 AND RowsWritten = 0)
               )
        ORDER BY RunDate DESC
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [days])
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


# ═════════════════════════════════════════════════════════════════════════════
#  REPORT VALIDATOR TOOLS  (Agent 2)
# ═════════════════════════════════════════════════════════════════════════════

def get_kpi_summary(days: int = 7) -> str:
    """Get a summary of all KPI values for recent days, including change %.

    Args:
        days: Lookback window. Default 7.

    Returns:
        JSON array with RunDate, KPIName, AggregationType, KPIValue,
        PreviousDayValue, DayOverDayChangePct, ExpectedMin, ExpectedMax.
    """
    query = f"""
        SELECT Id, RunDate, KPIName, AggregationType,
               KPIValue, PreviousDayValue, DayOverDayChangePct,
               ExpectedMin, ExpectedMax,
               PipelineName, PipelineStatus
        FROM   {_KPI}
        WHERE  RunDate >= DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
        ORDER BY RunDate DESC, KPIName
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [days])
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


def get_kpi_history(kpi_name: str, days: int = 60) -> str:
    """Get historical time series for a specific KPI.

    Args:
        kpi_name: Exact KPI name (e.g. 'Revenue', 'Revenue_YTD', 'Orders', 'Returns', 'AOV').
        days: Lookback window. Default 60.

    Returns:
        JSON array of rows ordered by date.
    """
    query = f"""
        SELECT RunDate, KPIName, AggregationType,
               KPIValue, PreviousDayValue, DayOverDayChangePct,
               ExpectedMin, ExpectedMax
        FROM   {_KPI}
        WHERE  KPIName = ?
               AND RunDate >= DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
        ORDER BY RunDate
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [kpi_name, days])
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


def get_ytd_violations(days: int = 30) -> str:
    """Find YTD KPI rows where the value decreased compared to the previous day.

    Args:
        days: Lookback window. Default 30.

    Returns:
        JSON array of rows where YTD decreased.
    """
    query = f"""
        SELECT Id, RunDate, KPIName, AggregationType,
               KPIValue, PreviousDayValue, DayOverDayChangePct
        FROM   {_KPI}
        WHERE  AggregationType = 'YTD'
               AND PreviousDayValue IS NOT NULL
               AND KPIValue < PreviousDayValue
               AND RunDate >= DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
        ORDER BY RunDate DESC
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [days])
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


def get_range_breaches(days: int = 30) -> str:
    """Find rows where KPIValue falls outside [ExpectedMin, ExpectedMax].

    Args:
        days: Lookback window. Default 30.

    Returns:
        JSON array of rows with range breaches.
    """
    query = f"""
        SELECT Id, RunDate, KPIName, AggregationType,
               KPIValue, ExpectedMin, ExpectedMax,
               PreviousDayValue, DayOverDayChangePct
        FROM   {_KPI}
        WHERE  RunDate >= DATEADD(DAY, -?, CAST(GETDATE() AS DATE))
               AND ExpectedMin IS NOT NULL
               AND ExpectedMax IS NOT NULL
               AND (KPIValue < ExpectedMin OR KPIValue > ExpectedMax)
        ORDER BY RunDate DESC
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [days])
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


def get_kpi_statistics(kpi_name: str) -> str:
    """Compute historical statistics for a KPI to understand its normal range.

    Args:
        kpi_name: Exact KPI name.

    Returns:
        JSON with avg, min, max, stddev of KPIValue and DayOverDayChangePct.
    """
    query = f"""
        SELECT KPIName, AggregationType,
               COUNT(*)               AS DataPoints,
               AVG(KPIValue)          AS AvgValue,
               MIN(KPIValue)          AS MinValue,
               MAX(KPIValue)          AS MaxValue,
               STDEV(KPIValue)        AS StdDevValue,
               AVG(DayOverDayChangePct)          AS AvgChangePct,
               MAX(ABS(DayOverDayChangePct))     AS MaxAbsChangePct
        FROM   {_KPI}
        WHERE  KPIName = ?
               AND KPIValue IS NOT NULL
        GROUP BY KPIName, AggregationType
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [kpi_name])
    result = json.dumps(_rows_to_dicts(cur), indent=2)
    cur.close()
    return result


# ═════════════════════════════════════════════════════════════════════════════
#  SHARED: FLAG ANOMALY + NOTIFY  (insert into agent_findings table)
# ═════════════════════════════════════════════════════════════════════════════

def flag_anomaly(
    row_id: int,
    anomaly_type: str,
    severity: str,
    description: str,
    hypothesis: str = "",
) -> str:
    """Flag a specific row as an anomaly by inserting into the agent_findings table.

    Args:
        row_id: The Id of the source row in daily_kpi_log.
        anomaly_type: One of PIPELINE_FAILURE, ROW_ANOMALY, KPI_DRIFT,
                      RANGE_BREACH, DURATION_ANOMALY, YTD_LOGIC_VIOLATION.
        severity: CRITICAL, HIGH, WARNING, or INFO.
        description: Human-readable anomaly description.
        hypothesis: Root-cause hypothesis (optional).

    Returns:
        JSON confirmation.
    """
    insert_query = f"""
        INSERT INTO {_FINDINGS}
            (SourceRowId, RunDate, PipelineName, KPIName, AggregationType,
             AgentName, AnomalyType, Severity, Description, Hypothesis)
        SELECT ?, RunDate, PipelineName, KPIName, AggregationType,
               'Agent', ?, ?, ?, ?
        FROM {_KPI}
        WHERE Id = ?
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(insert_query, [
        row_id, anomaly_type, severity, description, hypothesis, row_id,
    ])
    conn.commit()
    cur.close()
    return json.dumps({
        "status": "flagged",
        "row_id": row_id,
        "anomaly_type": anomaly_type,
        "severity": severity,
    })


def send_notification(
    row_id: int,
    team: str,
    users: str,
    message: str,
) -> str:
    """Record a notification for a flagged anomaly.

    Updates the most recent finding for this row_id with notification details.

    Args:
        row_id: The Id of the source row in daily_kpi_log.
        team: Team name to notify (e.g. 'Data Engineering', 'BI Consumers').
        users: Comma-separated list of recipients.
        message: The full notification message body.

    Returns:
        JSON confirmation.
    """
    query = f"""
        UPDATE {_FINDINGS}
        SET NotifiedTeam    = ?,
            NotifiedUsers   = ?,
            NotificationMsg = ?
        WHERE SourceRowId = ?
          AND DetectedAt = (
              SELECT MAX(DetectedAt) FROM {_FINDINGS}
              WHERE SourceRowId = ?
          )
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, [team, users, message, row_id, row_id])
    conn.commit()
    cur.close()
    return json.dumps({
        "status": "notified",
        "row_id": row_id,
        "team": team,
        "users": users,
    })
