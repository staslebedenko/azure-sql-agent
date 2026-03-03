-- ============================================================
-- Workshop: AI-Powered Data Quality Agents
-- ANOMALY INJECTION SCRIPT
-- ============================================================
-- Run this AFTER seed_data.sql while the demo loop is running.
-- The agents will detect these anomalies on their next cycle.
--
-- TIP: Run anomalies one at a time during the live demo to
--      show the agents reacting incrementally.
-- ============================================================

USE DataQualityWorkshop;
GO

-- ============================================================
-- ANOMALY 1: TODAY — Pipeline Failure (CRITICAL)
-- PL_Returns FAILED → Returns KPI = NULL (missing)
-- Agent 1 should detect: PIPELINE_FAILURE
-- Agent 2 should detect: missing KPI value
-- ============================================================
PRINT '💉 Injecting Anomaly 1: PL_Returns pipeline failure (today)...';

DECLARE @Today DATE = CAST(GETDATE() AS DATE);

UPDATE dbo.DailyDataQualityLog
SET PipelineStatus    = 'Failed',
    DurationSeconds   = 300,
    RowsRead          = 0,
    RowsWritten       = 0,
    ErrorMessage      = 'Connection timeout: Unable to connect to source database after 300 seconds. Retry attempts: 3/3 exhausted.',
    KPIValue          = NULL,
    PreviousDayValue  = (SELECT TOP 1 KPIValue FROM dbo.DailyDataQualityLog
                         WHERE KPIName = 'Returns' AND RunDate < @Today
                         ORDER BY RunDate DESC),
    DayOverDayChangePct = NULL
WHERE RunDate = @Today AND KPIName = 'Returns';

PRINT '   ✅ Done — PL_Returns is now Failed, Returns KPI = NULL';
GO

-- ============================================================
-- ANOMALY 2: DAY -3 — Zero rows written (HIGH)
-- PL_Orders succeeded but wrote 0 rows
-- Revenue = 0, Orders = 0, Revenue_YTD stale
-- ============================================================
PRINT '💉 Injecting Anomaly 2: Zero-row load on PL_Orders (day -3)...';

DECLARE @Day3 DATE = DATEADD(DAY, -3, CAST(GETDATE() AS DATE));

UPDATE dbo.DailyDataQualityLog
SET RowsRead          = 0,
    RowsWritten       = 0,
    KPIValue          = 0.00,
    DayOverDayChangePct = -100.00
WHERE RunDate = @Day3 AND KPIName IN ('Revenue', 'Orders');

UPDATE dbo.DailyDataQualityLog
SET KPIValue          = PreviousDayValue,
    DayOverDayChangePct = 0.00
WHERE RunDate = @Day3 AND KPIName = 'Revenue_YTD';

PRINT '   ✅ Done — Revenue & Orders = 0, Revenue_YTD stale';
GO

-- ============================================================
-- ANOMALY 3: DAY -7 — YTD decreased (CRITICAL)
-- Revenue_YTD dropped by 50K (logically impossible)
-- ============================================================
PRINT '💉 Injecting Anomaly 3: Revenue_YTD decreased (day -7)...';

DECLARE @Day7 DATE = DATEADD(DAY, -7, CAST(GETDATE() AS DATE));

UPDATE dbo.DailyDataQualityLog
SET KPIValue          = PreviousDayValue - 50000.00,
    DayOverDayChangePct = CASE
        WHEN PreviousDayValue != 0
        THEN CAST((-50000.00 / PreviousDayValue) * 100 AS DECIMAL(8,2))
        ELSE NULL END
WHERE RunDate = @Day7 AND KPIName = 'Revenue_YTD';

PRINT '   ✅ Done — Revenue_YTD shows impossible decrease';
GO

-- ============================================================
-- ANOMALY 4: DAY -14 — Extreme duration (WARNING)
-- PL_Transform took 900s instead of avg 180s
-- ============================================================
PRINT '💉 Injecting Anomaly 4: Duration spike on PL_Transform (day -14)...';

DECLARE @Day14 DATE = DATEADD(DAY, -14, CAST(GETDATE() AS DATE));

UPDATE dbo.DailyDataQualityLog
SET DurationSeconds = 900
WHERE RunDate = @Day14 AND PipelineName = 'PL_Transform';

PRINT '   ✅ Done — PL_Transform duration = 900s (5× avg)';
GO

-- ============================================================
-- ANOMALY 5: DAY -30 — AOV spike (WARNING)
-- AOV = $250 instead of normal ~$105
-- ============================================================
PRINT '💉 Injecting Anomaly 5: AOV spike (day -30)...';

DECLARE @Day30 DATE = DATEADD(DAY, -30, CAST(GETDATE() AS DATE));

UPDATE dbo.DailyDataQualityLog
SET KPIValue          = 250.00,
    DayOverDayChangePct = CASE
        WHEN PreviousDayValue != 0
        THEN CAST(((250.00 - PreviousDayValue) / PreviousDayValue) * 100 AS DECIMAL(8,2))
        ELSE NULL END
WHERE RunDate = @Day30 AND KPIName = 'AOV';

PRINT '   ✅ Done — AOV = $250 (2.4× normal)';
GO

-- ============================================================
-- VERIFICATION: show injected anomalies
-- ============================================================
PRINT '';
PRINT '🔍 Summary of injected anomalies:';

SELECT
    RunDate,
    PipelineName,
    PipelineStatus,
    KPIName,
    KPIValue,
    PreviousDayValue,
    DayOverDayChangePct,
    DurationSeconds,
    AvgDurationSeconds,
    ErrorMessage,
    CASE
        WHEN PipelineStatus = 'Failed' THEN '🔴 PIPELINE FAILURE'
        WHEN KPIValue = 0 AND ExpectedMin > 0 THEN '🟠 ZERO VALUE'
        WHEN AggregationType = 'YTD' AND KPIValue < PreviousDayValue THEN '🔴 YTD DECREASED'
        WHEN DurationSeconds > AvgDurationSeconds * 3 THEN '🟡 SLOW PIPELINE'
        WHEN KPIValue > ExpectedMax * 2 THEN '🟡 RANGE SPIKE'
        ELSE '✅ NORMAL'
    END AS ExpectedAgentFinding
FROM dbo.DailyDataQualityLog
WHERE PipelineStatus = 'Failed'
   OR (KPIValue = 0 AND ExpectedMin > 0)
   OR (AggregationType = 'YTD' AND KPIValue < ISNULL(PreviousDayValue, 0))
   OR (DurationSeconds > AvgDurationSeconds * 3)
   OR (KPIValue > ExpectedMax * 2)
   OR KPIValue IS NULL
ORDER BY RunDate DESC;
GO

PRINT '';
PRINT '💉 All 5 anomalies injected. Agents should detect them on next scan cycle.';
PRINT '   🔴 Today:   PL_Returns FAILED — Returns KPI missing';
PRINT '   🟠 Day -3:  PL_Orders wrote 0 rows — Revenue & Orders = 0';
PRINT '   🔴 Day -7:  Revenue_YTD decreased (impossible for cumulative KPI)';
PRINT '   🟡 Day -14: PL_Transform took 5× normal duration';
PRINT '   🟡 Day -30: AOV spiked to $250 (normal ~$105)';
GO
