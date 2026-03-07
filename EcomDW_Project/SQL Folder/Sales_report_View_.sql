USE [EcomDW]
GO


-- Reporting layer view that denormalizes FactSales with DimDate, DimProduct (SCD2 current),
-- and DimStore to expose sales measures, descriptive attributes, and lineage fields
-- for downstream reporting and analytics.
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE   VIEW [rpt].[vw_Sales]
AS
SELECT
    -- Fact identifiers
    f.FactSalesKey,
    f.DateKey,
    f.ProductKey,
    f.StoreKey,

    -- Date (report-friendly)
    dd.[Date]        AS SaleDate,
    dd.[Year],
    dd.[Quarter],
    dd.[Month],
    dd.[MonthName],
    dd.[Day],
    dd.[DayName],
    dd.[IsWeekend],

    -- Product (SCD2 dimension)
    dp.ProductNaturalId AS ProductId,
    dp.ProductName,
    dp.Category,
    dp.Brand,

    -- Store (Type 1 dimension)
    ds.StoreNaturalId   AS StoreId,
    ds.StoreName,
    ds.City,
    ds.[State],
    ds.Region,

    -- Measures
    f.Units    AS Units,
    f.Revenue  AS Revenue,

    -- Lineage / audit
    f.LoadDttm,
    f.SourceFile
FROM dw.FactSales f
JOIN dw.DimDate dd
    ON dd.DateKey = f.DateKey

-- Product SCD2 "current" row
JOIN dw.DimProduct dp
    ON dp.ProductKey = f.ProductKey
   AND dp.EndDate IS NULL   -- assumes "current row" = EndDate IS NULL

-- Store Type 1 (no SCD filter)
JOIN dw.DimStore ds
    ON ds.StoreKey = f.StoreKey;
GO


