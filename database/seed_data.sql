-- ============================================================  
-- Workshop: AI-Powered Data Quality Agents  
-- Database Initialization Script (CLEAN DATA ONLY)  
-- ============================================================  
-- Run against: SQL Server (Azure SQL, local, or Docker)  
-- Creates: 1 table, ~300 rows (60 days × 5 KPIs)  
-- All data is NORMAL — no anomalies planted here.  
-- Run inject_anomalies.sql separately to trigger agent alerts.  
-- ============================================================  
  
USE master;  
GO  
  
-- Create database (skip if using existing)  
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataQualityWorkshop')  
    CREATE DATABASE DataQualityWorkshop;  
GO  
  
USE DataQualityWorkshop;  
GO  
  
-- Drop table if re-running  
IF OBJECT_ID('dbo.DailyDataQualityLog', 'U') IS NOT NULL  
    DROP TABLE dbo.DailyDataQualityLog;  
GO  
  
-- ============================================================  
-- SINGLE TABLE: combines pipeline metadata + KPI values  
-- ============================================================  
CREATE TABLE dbo.DailyDataQualityLog  
(  
    Id                    INT IDENTITY(1,1) PRIMARY KEY,  
  
    -- Pipeline context (Agent 1 focuses here)  
    RunDate               DATE           NOT NULL,  
    PipelineName          VARCHAR(100)   NOT NULL,  
    PipelineStatus        VARCHAR(20)    NOT NULL,  -- Succeeded / Failed  
    DurationSeconds       INT            NULL,  
    AvgDurationSeconds    INT            NULL,       -- historical avg for comparison  
    RowsRead              INT            NULL,  
    RowsWritten           INT            NULL,  
    ErrorMessage          VARCHAR(500)   NULL,  
  
    -- KPI context (Agent 2 focuses here)  
    KPIName               VARCHAR(50)    NOT NULL,  
    KPIValue              DECIMAL(18,2)  NULL,       -- NULL = missing/failed  
    PreviousDayValue      DECIMAL(18,2)  NULL,  
    DayOverDayChangePct   DECIMAL(8,2)   NULL,       -- % change from previous day  
    ExpectedMin           DECIMAL(18,2)  NULL,  
    ExpectedMax           DECIMAL(18,2)  NULL,  
    AggregationType       VARCHAR(10)    NOT NULL,   -- Daily / QTD / YTD  
  
    -- Agent output columns (agents WRITE here)  
    AnomalyDetected       BIT            DEFAULT 0,  
    AnomalyType           VARCHAR(50)    NULL,  
    AnomalyDescription    VARCHAR(500)   NULL,  
    Severity              VARCHAR(20)    NULL,  
    Hypothesis            VARCHAR(500)   NULL,  
    NotifiedTeam          VARCHAR(100)   NULL,  
    NotifiedUsers         VARCHAR(200)   NULL,  
    NotificationMessage   VARCHAR(2000)  NULL  
);  
GO  
  
-- ============================================================  
-- SEED DATA GENERATION  
-- ============================================================  
  
DECLARE @StartDate DATE = DATEADD(DAY, -59, CAST(GETDATE() AS DATE));  
DECLARE @EndDate   DATE = CAST(GETDATE() AS DATE);  
DECLARE @CurrentDate DATE = @StartDate;  
  
WHILE @CurrentDate <= @EndDate  
BEGIN  
    DECLARE @DayOfYear    INT = DATEPART(DAYOFYEAR, @CurrentDate);  
    DECLARE @DayOfWeek    INT = DATEPART(WEEKDAY, @CurrentDate);  
    DECLARE @IsWeekend    BIT = CASE WHEN @DayOfWeek IN (1, 7) THEN 1 ELSE 0 END;  
    DECLARE @WeekendFactor FLOAT = CASE WHEN @IsWeekend = 1 THEN 0.6 ELSE 1.0 END;  
    DECLARE @Jitter FLOAT = (SIN(@DayOfYear * 3.14159) * 0.15);  
  
    -- KPI 1: Revenue (Daily) — range 80k–150k  
    DECLARE @RevenueBase FLOAT = 115000.0;  
    DECLARE @Revenue DECIMAL(18,2) = CAST(  
        @RevenueBase * @WeekendFactor * (1.0 + @Jitter) AS DECIMAL(18,2));  
  
    DECLARE @PrevRevenue DECIMAL(18,2) = (  
        SELECT TOP 1 KPIValue FROM dbo.DailyDataQualityLog  
        WHERE KPIName = 'Revenue' AND RunDate < @CurrentDate  
        ORDER BY RunDate DESC);  
  
    DECLARE @RevChangePct DECIMAL(8,2) = CASE  
        WHEN @PrevRevenue IS NOT NULL AND @PrevRevenue != 0  
        THEN CAST(((@Revenue - @PrevRevenue) / @PrevRevenue) * 100 AS DECIMAL(8,2))  
        ELSE NULL END;  
  
    INSERT INTO dbo.DailyDataQualityLog  
        (RunDate, PipelineName, PipelineStatus, DurationSeconds, AvgDurationSeconds,  
         RowsRead, RowsWritten, ErrorMessage,  
         KPIName, KPIValue, PreviousDayValue, DayOverDayChangePct,  
         ExpectedMin, ExpectedMax, AggregationType)  
    VALUES  
        (@CurrentDate, 'PL_Orders', 'Succeeded',  
         120 + ABS(CHECKSUM(NEWID())) % 60, 150,  
         CAST(@Revenue / 100 AS INT), CAST(@Revenue / 100 AS INT), NULL,  
         'Revenue', @Revenue, @PrevRevenue, @RevChangePct,  
         80000.00, 150000.00, 'Daily');  
  
    -- KPI 2: Revenue_YTD (cumulative, should always increase)  
    DECLARE @PrevYTD DECIMAL(18,2) = ISNULL((  
        SELECT TOP 1 KPIValue FROM dbo.DailyDataQualityLog  
        WHERE KPIName = 'Revenue_YTD' AND RunDate < @CurrentDate  
        ORDER BY RunDate DESC), 0);  
  
    DECLARE @RevenueYTD DECIMAL(18,2) = CASE  
        WHEN MONTH(@CurrentDate) = 1 AND DAY(@CurrentDate) = 1 THEN @Revenue  
        ELSE @PrevYTD + @Revenue END;  
  
    DECLARE @YTDChangePct DECIMAL(8,2) = CASE  
        WHEN @PrevYTD IS NOT NULL AND @PrevYTD != 0  
        THEN CAST(((@RevenueYTD - @PrevYTD) / @PrevYTD) * 100 AS DECIMAL(8,2))  
        ELSE NULL END;  
  
    INSERT INTO dbo.DailyDataQualityLog  
        (RunDate, PipelineName, PipelineStatus, DurationSeconds, AvgDurationSeconds,  
         RowsRead, RowsWritten, ErrorMessage,  
         KPIName, KPIValue, PreviousDayValue, DayOverDayChangePct,  
         ExpectedMin, ExpectedMax, AggregationType)  
    VALUES  
        (@CurrentDate, 'PL_Transform', 'Succeeded',  
         170 + ABS(CHECKSUM(NEWID())) % 20, 180,  
         0, 0, NULL,  
         'Revenue_YTD', @RevenueYTD, @PrevYTD, @YTDChangePct,  
         NULL, NULL, 'YTD');  
  
    -- KPI 3: Orders (Daily) — range 800–1500  
    DECLARE @OrdersBase FLOAT = 1150.0;  
    DECLARE @Orders DECIMAL(18,2) = CAST(  
        @OrdersBase * @WeekendFactor * (1.0 + @Jitter * 0.8) AS DECIMAL(18,2));  
  
    DECLARE @PrevOrders DECIMAL(18,2) = (  
        SELECT TOP 1 KPIValue FROM dbo.DailyDataQualityLog  
        WHERE KPIName = 'Orders' AND RunDate < @CurrentDate  
        ORDER BY RunDate DESC);  
  
    DECLARE @OrdChangePct DECIMAL(8,2) = CASE  
        WHEN @PrevOrders IS NOT NULL AND @PrevOrders != 0  
        THEN CAST(((@Orders - @PrevOrders) / @PrevOrders) * 100 AS DECIMAL(8,2))  
        ELSE NULL END;  
  
    INSERT INTO dbo.DailyDataQualityLog  
        (RunDate, PipelineName, PipelineStatus, DurationSeconds, AvgDurationSeconds,  
         RowsRead, RowsWritten, ErrorMessage,  
         KPIName, KPIValue, PreviousDayValue, DayOverDayChangePct,  
         ExpectedMin, ExpectedMax, AggregationType)  
    VALUES  
        (@CurrentDate, 'PL_Orders', 'Succeeded',  
         120 + ABS(CHECKSUM(NEWID())) % 60, 150,  
         CAST(@Orders AS INT), CAST(@Orders AS INT), NULL,  
         'Orders', @Orders, @PrevOrders, @OrdChangePct,  
         800.00, 1500.00, 'Daily');  
  
    -- KPI 4: Returns (Daily) — range 20–80  
    DECLARE @ReturnsBase FLOAT = 45.0;  
    DECLARE @Returns DECIMAL(18,2) = CAST(  
        @ReturnsBase * @WeekendFactor * (1.0 + @Jitter * 1.5) AS DECIMAL(18,2));  
  
    DECLARE @PrevReturns DECIMAL(18,2) = (  
        SELECT TOP 1 KPIValue FROM dbo.DailyDataQualityLog  
        WHERE KPIName = 'Returns' AND RunDate < @CurrentDate  
        ORDER BY RunDate DESC);  
  
    DECLARE @RetChangePct DECIMAL(8,2) = CASE  
        WHEN @PrevReturns IS NOT NULL AND @PrevReturns != 0  
        THEN CAST(((@Returns - @PrevReturns) / @PrevReturns) * 100 AS DECIMAL(8,2))  
        ELSE NULL END;  
  
    INSERT INTO dbo.DailyDataQualityLog  
        (RunDate, PipelineName, PipelineStatus, DurationSeconds, AvgDurationSeconds,  
         RowsRead, RowsWritten, ErrorMessage,  
         KPIName, KPIValue, PreviousDayValue, DayOverDayChangePct,  
         ExpectedMin, ExpectedMax, AggregationType)  
    VALUES  
        (@CurrentDate, 'PL_Returns', 'Succeeded',  
         40 + ABS(CHECKSUM(NEWID())) % 20, 50,  
         CAST(@Returns AS INT), CAST(@Returns AS INT), NULL,  
         'Returns', @Returns, @PrevReturns, @RetChangePct,  
         20.00, 80.00, 'Daily');  
  
    -- KPI 5: AOV (Average Order Value) — range 95–115  
    DECLARE @AOV DECIMAL(18,2) = CASE  
        WHEN @Orders > 0 THEN CAST(@Revenue / @Orders AS DECIMAL(18,2))  
        ELSE NULL END;  
  
    DECLARE @PrevAOV DECIMAL(18,2) = (  
        SELECT TOP 1 KPIValue FROM dbo.DailyDataQualityLog  
        WHERE KPIName = 'AOV' AND RunDate < @CurrentDate  
        ORDER BY RunDate DESC);  
  
    DECLARE @AOVChangePct DECIMAL(8,2) = CASE  
        WHEN @PrevAOV IS NOT NULL AND @PrevAOV != 0  
        THEN CAST(((@AOV - @PrevAOV) / @PrevAOV) * 100 AS DECIMAL(8,2))  
        ELSE NULL END;  
  
    INSERT INTO dbo.DailyDataQualityLog  
        (RunDate, PipelineName, PipelineStatus, DurationSeconds, AvgDurationSeconds,  
         RowsRead, RowsWritten, ErrorMessage,  
         KPIName, KPIValue, PreviousDayValue, DayOverDayChangePct,  
         ExpectedMin, ExpectedMax, AggregationType)  
    VALUES  
        (@CurrentDate, 'PL_Transform', 'Succeeded',  
         170 + ABS(CHECKSUM(NEWID())) % 20, 180,  
         0, 0, NULL,  
         'AOV', @AOV, @PrevAOV, @AOVChangePct,  
         95.00, 115.00, 'Daily');  
  
    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);  
END;  
GO  
  
-- ============================================================  
-- VERIFICATION (all data should be clean / normal)  
-- ============================================================  
SELECT COUNT(*) AS TotalRows FROM dbo.DailyDataQualityLog;  
  
SELECT KPIName, COUNT(*) AS Days, MIN(KPIValue) AS MinVal, MAX(KPIValue) AS MaxVal  
FROM dbo.DailyDataQualityLog GROUP BY KPIName;  
  
-- Confirm: zero anomalies expected  
SELECT COUNT(*) AS AnomalousRows  
FROM dbo.DailyDataQualityLog  
WHERE PipelineStatus = 'Failed'  
   OR (KPIValue = 0 AND ExpectedMin > 0)  
   OR (AggregationType = 'YTD' AND KPIValue < ISNULL(PreviousDayValue, 0))  
   OR (DurationSeconds > AvgDurationSeconds * 3)  
   OR (KPIValue > ExpectedMax * 2)  
   OR KPIValue IS NULL;  
GO  
  
PRINT '✅ Database initialized with 60 days of CLEAN data (no anomalies).';  
PRINT '   Run inject_anomalies.sql to plant problems for agents to discover.';  
GO  
