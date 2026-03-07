
/*
Purpose: Performs an incremental upsert into dw.FactSales.
Description:
Resolving staged sales records to Date, Product, and Store dimension keys. 
The process updates existing fact rows only when newer source data is available, 
inserts brand-new sales records, and logs row counts for ETL monitoring and auditability.

Source: stg.vSalesRaw_Dedup
Target: dw.FactSales
Load Type: Incremental fact upsert
Grain: One row per DateKey, ProductKey, StoreKey
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @RunIdTest NVARCHAR(40) = ?;   -- only parameter now
DECLARE @PackageStartDt DATETIME2(3) = SYSDATETIME();

BEGIN TRY
    BEGIN TRAN;

    /* 1) UPDATE existing rows (only if staging row is newer) */
    UPDATE f
        SET f.Units      = s.Units,
            f.Revenue    = s.Revenue,
            f.LoadDttm   = @PackageStartDt,
            f.SourceFile = s.SourceFile
    FROM dw.FactSales f
    JOIN dw.DimProduct dp 
        ON dp.ProductKey = f.ProductKey
       AND dp.EndDate IS NULL
    JOIN dw.DimStore ds 
        ON ds.StoreKey = f.StoreKey
    JOIN dw.DimDate dd 
        ON dd.DateKey = f.DateKey
    JOIN stg.vSalesRaw_Dedup s
        ON s.ProductId = dp.ProductNaturalId
       AND s.StoreId   = ds.StoreNaturalId
       AND s.SaleDate  = dd.[Date]
    WHERE s.LoadDttm > f.LoadDttm;

    DECLARE @UpdateCount INT = @@ROWCOUNT;

    /* 2) INSERT brand-new rows */
    INSERT INTO dw.FactSales
        (DateKey, ProductKey, StoreKey, Units, Revenue, LoadDttm, SourceFile)
    SELECT
        dd.DateKey,
        dp.ProductKey,
        ds.StoreKey,
        s.Units,
        s.Revenue,
        @PackageStartDt,
        s.SourceFile
    FROM stg.vSalesRaw_Dedup s
    JOIN dw.DimProduct dp
        ON dp.ProductNaturalId = s.ProductId
       AND dp.EndDate IS NULL
    JOIN dw.DimStore ds
        ON ds.StoreNaturalId = s.StoreId
    JOIN dw.DimDate dd
        ON dd.[Date] = s.SaleDate
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dw.FactSales f
        WHERE f.DateKey    = dd.DateKey
          AND f.ProductKey = dp.ProductKey
          AND f.StoreKey   = ds.StoreKey
    );

    DECLARE @InsertCount INT = @@ROWCOUNT;

    /* 3) Log counts */
    INSERT INTO etl.RunLog (RunId, StepName, [RowCount], Status, LoggedAt)
    VALUES (@RunIdTest, 'FactSales UPDATE', @UpdateCount, 'Succeeded', SYSDATETIME());

    INSERT INTO etl.RunLog (RunId, StepName, [RowCount], Status, LoggedAt)
    VALUES (@RunIdTest, 'FactSales INSERT', @InsertCount, 'Succeeded', SYSDATETIME());

    COMMIT;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK;
    THROW;
END CATCH;
