USE [EcomDW]
GO

-- Helps remove duplicates from stg.SalesRaw table--


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--Create a deduped view--


CREATE   VIEW [stg].[vSalesRaw_Dedup]
AS
WITH ranked AS (
  SELECT
      s.*,
      ROW_NUMBER() OVER (
        PARTITION BY s.SaleDate, s.ProductId, s.StoreId
        ORDER BY
          CASE WHEN s.LoadDttm IS NULL THEN 1 ELSE 0 END, -- NULLs last
          s.LoadDttm DESC,
          s.SourceFile DESC
      ) AS rn
  FROM stg.SalesRaw s
)
SELECT *
FROM ranked
WHERE rn = 1;
GO


