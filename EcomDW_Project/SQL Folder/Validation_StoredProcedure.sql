USE [ECommDW]
GO

/*
Purpose: 
Central ETL validation procedure that evaluates data quality, referential integrity,
grain enforcement, domain compliance, master data completeness, and pricing outliers across
staging and warehouse layers. The procedure returns structured PASS/WARN/FAIL results and
optionally enforces a fail gate to stop downstream processing when critical thresholds are exceeded.

Description:
Central ETL validation procedure that evaluates data quality, referential integrity,
grain enforcement, domain compliance, master data completeness, and pricing outliers across
staging and warehouse layers. 

Source Objects: stg.SalesRaw, stg.vSalesRaw_Dedup, stg.StoreMaster
Target / Validated Objects: dw.FactSales, dw.DimProduct, dw.DimStore, dw.DimDate
Load Role: ETL validation and quality gate
*/



*/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etl].[usp_ValidateLoad2]
(
    @FailOnGate BIT = 1,

    @MaxMissingProduct INT = 0,
    @MaxMissingStore   INT = 0,
    @MaxMissingDate    INT = 0,
    @MaxFactOrphans    INT = 0,
    @MaxFactDuplicates INT = 0,
    @MaxStgGrainDups   INT = 0,

    -- StoreMaster checks
    @MaxMissingStoreMaster INT = 0,
    @MaxStoreMasterNulls   INT = 0,  -- null Region/State/City among stores used in staged data

    -- Domain checks
    @MaxBadUnits    INT = 0,          -- Units <= 0
    @MaxBadRevenue  INT = 0,          -- Revenue < 0
    @MaxZeroRevenueWithUnits INT = 0, -- Revenue=0 while Units>0 (often promo/free) - usually WARN

    -- Unit price outliers (WARN)
    @UnitPriceHigh DECIMAL(12,2) = 500.00,
    @UnitPriceLow  DECIMAL(12,2) = 0.50,

    -- NEW: empty inbound policy
    @NoDataStatus VARCHAR(10) = 'WARN', -- PASS | WARN | FAIL

    @ReturnSamples BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#ValidationResults') IS NOT NULL DROP TABLE #ValidationResults;

    CREATE TABLE #ValidationResults
    (
        CheckGroup  VARCHAR(40)  NOT NULL,
        CheckName   VARCHAR(120) NOT NULL,
        MetricValue DECIMAL(18,2) NULL,
        Threshold   DECIMAL(18,2) NULL,
        Status      VARCHAR(10)  NOT NULL,
        Notes       VARCHAR(400) NULL
    );

    -------------------------------------------------------------------
    -- 0) Row counts (informational)
    -------------------------------------------------------------------
    DECLARE @StgSalesRaw BIGINT = (SELECT COUNT(*) FROM stg.SalesRaw);
    DECLARE @StgDedup    BIGINT = (SELECT COUNT(*) FROM stg.vSalesRaw_Dedup);
    DECLARE @FactRows    BIGINT = (SELECT COUNT(*) FROM dw.FactSales);

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
      ('RowCounts','stg.SalesRaw rowcount',      @StgSalesRaw, NULL, 'PASS', NULL),
      ('RowCounts','stg.vSalesRaw_Dedup rowcount',@StgDedup,   NULL, 'PASS', NULL),
      ('RowCounts','dw.FactSales rowcount',      @FactRows,    NULL, 'PASS', NULL);

    -------------------------------------------------------------------
    -- 0b) If NO staged rows, short-circuit based on policy
    -------------------------------------------------------------------
    IF @StgDedup = 0
    BEGIN
        INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
        VALUES
        (
            'Summary',
            'No staged rows to validate (empty inbound or no landed data)',
            0, NULL,
            CASE WHEN UPPER(@NoDataStatus) IN ('PASS','WARN','FAIL') THEN UPPER(@NoDataStatus) ELSE 'PASS' END,
            CONCAT('Policy=@NoDataStatus=', @NoDataStatus, '; No downstream checks executed.')
        );

        SELECT CheckGroup, CheckName, MetricValue, Threshold, Status, Notes
        FROM #ValidationResults
        ORDER BY CASE Status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
                 CheckGroup, CheckName;

        IF @FailOnGate = 1 AND EXISTS (SELECT 1 FROM #ValidationResults WHERE Status = 'FAIL')
            THROW 51000, 'Validation gate FAILED: empty staged set per policy.', 1;

        RETURN;
    END

    -------------------------------------------------------------------
    -- 1) Staging grain duplicates (SaleDate, ProductId, StoreId)
    -------------------------------------------------------------------
    DECLARE @StgGrainDups INT = 0;

    SELECT @StgGrainDups = COUNT(*)
    FROM
    (
        SELECT SaleDate, ProductId, StoreId
        FROM stg.vSalesRaw_Dedup
        GROUP BY SaleDate, ProductId, StoreId
        HAVING COUNT(*) > 1
    ) AS d;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
    (
        'Grain',
        'Staging dedup grain duplicates (SaleDate,ProductId,StoreId)',
        @StgGrainDups,
        @MaxStgGrainDups,
        CASE WHEN @StgGrainDups <= @MaxStgGrainDups THEN 'PASS' ELSE 'FAIL' END,
        'Expect 0: vSalesRaw_Dedup should already be 1 row per grain'
    );

    -------------------------------------------------------------------
    -- 2) Fact grain duplicates (DateKey, StoreKey, ProductKey)
    -------------------------------------------------------------------
    DECLARE @FactGrainDups INT = 0;

    SELECT @FactGrainDups = COUNT(*)
    FROM
    (
        SELECT DateKey, StoreKey, ProductKey
        FROM dw.FactSales
        GROUP BY DateKey, StoreKey, ProductKey
        HAVING COUNT(*) > 1
    ) AS d;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
    (
        'Grain',
        'Fact grain duplicates (DateKey,StoreKey,ProductKey)',
        @FactGrainDups,
        @MaxFactDuplicates,
        CASE WHEN @FactGrainDups <= @MaxFactDuplicates THEN 'PASS' ELSE 'FAIL' END,
        'Expect 0: grain should be enforced (often via unique index)'
    );

    -------------------------------------------------------------------
    -- 3) Missing dimension lookups from staged dedup
    -------------------------------------------------------------------
    DECLARE @MissingProduct INT = 0, @MissingStore INT = 0, @MissingDate INT = 0, @TotalStaged INT = 0;

    SELECT
        @TotalStaged = COUNT(*),
        @MissingProduct = ISNULL(SUM(CASE WHEN dp.ProductKey IS NULL THEN 1 ELSE 0 END),0),
        @MissingStore   = ISNULL(SUM(CASE WHEN ds.StoreKey   IS NULL THEN 1 ELSE 0 END),0),
        @MissingDate    = ISNULL(SUM(CASE WHEN dd.DateKey    IS NULL THEN 1 ELSE 0 END),0)
    FROM stg.vSalesRaw_Dedup s
    LEFT JOIN dw.DimProduct dp ON dp.ProductNaturalId = s.ProductId
    LEFT JOIN dw.DimStore   ds ON ds.StoreNaturalId   = s.StoreId
    LEFT JOIN dw.DimDate    dd ON dd.[Date]           = s.SaleDate;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
      ('FK','Missing ProductKey from staging', @MissingProduct, @MaxMissingProduct,
        CASE WHEN @MissingProduct <= @MaxMissingProduct THEN 'PASS' ELSE 'FAIL' END,
        CONCAT('Total staged: ', @TotalStaged)),
      ('FK','Missing StoreKey from staging', @MissingStore, @MaxMissingStore,
        CASE WHEN @MissingStore <= @MaxMissingStore THEN 'PASS' ELSE 'FAIL' END,
        CONCAT('Total staged: ', @TotalStaged)),
      ('FK','Missing DateKey from staging', @MissingDate, @MaxMissingDate,
        CASE WHEN @MissingDate <= @MaxMissingDate THEN 'PASS' ELSE 'FAIL' END,
        CONCAT('Total staged: ', @TotalStaged));

    -------------------------------------------------------------------
    -- 4) Orphans inside FactSales (should be 0)
    -------------------------------------------------------------------
    DECLARE @OrphanFact INT = 0;

    SELECT @OrphanFact =
        ISNULL(SUM(CASE WHEN dp.ProductKey IS NULL THEN 1 ELSE 0 END),0) +
        ISNULL(SUM(CASE WHEN ds.StoreKey   IS NULL THEN 1 ELSE 0 END),0) +
        ISNULL(SUM(CASE WHEN dd.DateKey    IS NULL THEN 1 ELSE 0 END),0)
    FROM dw.FactSales f
    LEFT JOIN dw.DimProduct dp ON dp.ProductKey = f.ProductKey
    LEFT JOIN dw.DimStore   ds ON ds.StoreKey   = f.StoreKey
    LEFT JOIN dw.DimDate    dd ON dd.DateKey    = f.DateKey;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
    (
        'FK',
        'Orphan keys inside FactSales (any dim missing)',
        @OrphanFact,
        @MaxFactOrphans,
        CASE WHEN @OrphanFact <= @MaxFactOrphans THEN 'PASS' ELSE 'FAIL' END,
        'Expect 0: FactSales keys should resolve to dims'
    );

    -------------------------------------------------------------------
    -- 5) StoreMaster coverage
    -------------------------------------------------------------------
    DECLARE @MissingStoreMaster INT = 0;

    SELECT @MissingStoreMaster = COUNT(*)
    FROM (
        SELECT DISTINCT s.StoreId
        FROM stg.vSalesRaw_Dedup s
        WHERE s.StoreId IS NOT NULL
    ) x
    LEFT JOIN stg.StoreMaster sm
        ON sm.StoreId = x.StoreId
    WHERE sm.StoreId IS NULL;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
    (
        'MasterData',
        'Staged StoreIds missing from stg.StoreMaster',
        @MissingStoreMaster,
        @MaxMissingStoreMaster,
        CASE WHEN @MissingStoreMaster <= @MaxMissingStoreMaster THEN 'PASS' ELSE 'FAIL' END,
        'Fail prevents missing Region/State/City mapping from breaking DimStore loads'
    );

    -------------------------------------------------------------------
    -- 6) StoreMaster completeness (WARN by default)
    -------------------------------------------------------------------
    DECLARE @StoreMasterNulls INT = 0;

    SELECT @StoreMasterNulls = COUNT(*)
    FROM (
        SELECT DISTINCT s.StoreId
        FROM stg.vSalesRaw_Dedup s
        WHERE s.StoreId IS NOT NULL
    ) x
    JOIN stg.StoreMaster sm
        ON sm.StoreId = x.StoreId
    WHERE sm.Region IS NULL OR sm.[State] IS NULL OR sm.City IS NULL;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
    (
        'MasterData',
        'StoreMaster null Region/State/City for used stores',
        @StoreMasterNulls,
        @MaxStoreMasterNulls,
        CASE WHEN @StoreMasterNulls <= @MaxStoreMasterNulls THEN 'PASS' ELSE 'WARN' END,
        'WARN: descriptive attributes missing; keys may still load but reporting filters degrade'
    );

    -------------------------------------------------------------------
    -- 7) Domain checks
    -------------------------------------------------------------------
    DECLARE @BadUnits INT = 0, @BadRevenue INT = 0, @ZeroRevenueWithUnits INT = 0;

    SELECT
        @BadUnits = ISNULL(SUM(CASE WHEN ISNULL(s.Units,0) <= 0 THEN 1 ELSE 0 END),0),
        @BadRevenue = ISNULL(SUM(CASE WHEN ISNULL(s.Revenue,0) < 0 THEN 1 ELSE 0 END),0),
        @ZeroRevenueWithUnits = ISNULL(SUM(CASE WHEN ISNULL(s.Units,0) > 0 AND ISNULL(s.Revenue,0) = 0 THEN 1 ELSE 0 END),0)
    FROM stg.vSalesRaw_Dedup s;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
      ('Domain','Units <= 0 in staging dedup', @BadUnits, @MaxBadUnits,
        CASE WHEN @BadUnits <= @MaxBadUnits THEN 'PASS' ELSE 'FAIL' END,
        'FAIL: sales units should be positive for this model'),
      ('Domain','Revenue < 0 in staging dedup', @BadRevenue, @MaxBadRevenue,
        CASE WHEN @BadRevenue <= @MaxBadRevenue THEN 'PASS' ELSE 'FAIL' END,
        'FAIL: negative revenue not allowed for this model'),
      ('Domain','Revenue = 0 while Units > 0', @ZeroRevenueWithUnits, @MaxZeroRevenueWithUnits,
        CASE WHEN @ZeroRevenueWithUnits <= @MaxZeroRevenueWithUnits THEN 'PASS' ELSE 'WARN' END,
        'WARN: could be promo/free; flag for review');

    -------------------------------------------------------------------
    -- 8) Unit price outliers (WARN)
    -------------------------------------------------------------------
    DECLARE @UnitPriceTooHigh INT = 0, @UnitPriceTooLow INT = 0;

    ;WITH p AS (
        SELECT
            UnitPrice = CAST(s.Revenue / NULLIF(CAST(s.Units AS decimal(18,2)),0) AS decimal(18,2))
        FROM stg.vSalesRaw_Dedup s
        WHERE s.Units IS NOT NULL AND s.Units <> 0
          AND s.Revenue IS NOT NULL
          AND s.Revenue >= 0
    )
    SELECT
        @UnitPriceTooHigh = ISNULL(SUM(CASE WHEN UnitPrice > @UnitPriceHigh THEN 1 ELSE 0 END),0),
        @UnitPriceTooLow  = ISNULL(SUM(CASE WHEN UnitPrice < @UnitPriceLow  THEN 1 ELSE 0 END),0)
    FROM p;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
      ('Outliers','UnitPrice above high threshold (WARN)', @UnitPriceTooHigh, @UnitPriceHigh,
        CASE WHEN @UnitPriceTooHigh = 0 THEN 'PASS' ELSE 'WARN' END,
        'UnitPrice = Revenue/Units; warns on unlikely spikes'),
      ('Outliers','UnitPrice below low threshold (WARN)', @UnitPriceTooLow, @UnitPriceLow,
        CASE WHEN @UnitPriceTooLow = 0 THEN 'PASS' ELSE 'WARN' END,
        'Warns on near-zero pricing; could be promo or bad data');

    -------------------------------------------------------------------
    -- 9) Dimension natural-key duplicate checks (FAIL)
    -------------------------------------------------------------------
    DECLARE @DupProdNK INT = 0, @DupStoreNK INT = 0, @DupDate INT = 0;

    SELECT @DupProdNK = COUNT(*)
    FROM (
        SELECT ProductNaturalId
        FROM dw.DimProduct
        GROUP BY ProductNaturalId
        HAVING COUNT(*) > 1
    ) d;

    SELECT @DupStoreNK = COUNT(*)
    FROM (
        SELECT StoreNaturalId
        FROM dw.DimStore
        GROUP BY StoreNaturalId
        HAVING COUNT(*) > 1
    ) d;

    SELECT @DupDate = COUNT(*)
    FROM (
        SELECT [Date]
        FROM dw.DimDate
        GROUP BY [Date]
        HAVING COUNT(*) > 1
    ) d;

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
      ('Uniqueness','DimProduct duplicate ProductNaturalId groups', @DupProdNK, 0,
        CASE WHEN @DupProdNK = 0 THEN 'PASS' ELSE 'FAIL' END,
        'Expect 0: natural key must be unique'),
      ('Uniqueness','DimStore duplicate StoreNaturalId groups', @DupStoreNK, 0,
        CASE WHEN @DupStoreNK = 0 THEN 'PASS' ELSE 'FAIL' END,
        'Expect 0: natural key must be unique'),
      ('Uniqueness','DimDate duplicate [Date] groups', @DupDate, 0,
        CASE WHEN @DupDate = 0 THEN 'PASS' ELSE 'FAIL' END,
        'Expect 0: date must be unique');

    -------------------------------------------------------------------
    -- 10) Summary row (overall)
    -------------------------------------------------------------------
    DECLARE @FailCount INT = (SELECT COUNT(*) FROM #ValidationResults WHERE Status = 'FAIL');
    DECLARE @WarnCount INT = (SELECT COUNT(*) FROM #ValidationResults WHERE Status = 'WARN');

    INSERT INTO #ValidationResults (CheckGroup, CheckName, MetricValue, Threshold, Status, Notes)
    VALUES
    (
        'Summary',
        'Overall validation status',
        CAST(@FailCount AS decimal(18,2)),
        NULL,
        CASE WHEN @FailCount > 0 THEN 'FAIL'
             WHEN @WarnCount > 0 THEN 'WARN'
             ELSE 'PASS' END,
        CONCAT('FAILs=', @FailCount, '; WARNs=', @WarnCount)
    );

    -------------------------------------------------------------------
    -- Output compact results
    -------------------------------------------------------------------
    SELECT CheckGroup, CheckName, MetricValue, Threshold, Status, Notes
    FROM #ValidationResults
    ORDER BY
        CASE Status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
        CheckGroup, CheckName;

    -------------------------------------------------------------------
    -- FAIL GATE
    -------------------------------------------------------------------
    IF @FailOnGate = 1
    BEGIN
        IF EXISTS (SELECT 1 FROM #ValidationResults WHERE Status = 'FAIL')
            THROW 51000, 'Validation gate FAILED: one or more critical checks returned FAIL.', 1;
    END
END
GO


