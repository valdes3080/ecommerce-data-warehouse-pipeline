USE [EcomDW]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
-----Landing Table for monthly sales-----


CREATE TABLE [stg].[SalesRaw](
	[SaleDate] [date] NULL,
	[ProductId] [varchar](50) NULL,
	[ProductName] [varchar](200) NULL,
	[Category] [varchar](100) NULL,
	[Brand] [varchar](100) NULL,
	[StoreId] [varchar](50) NULL,
	[StoreName] [varchar](200) NULL,
	[City] [varchar](100) NULL,
	[State] [varchar](50) NULL,
	[Units] [int] NULL,
	[Revenue] [decimal](12, 2) NULL,
	[SourceFile] [nvarchar](260) NULL,
	[LoadDttm] [datetime2](3) NOT NULL
) ON [PRIMARY]
GO

ALTER TABLE [stg].[SalesRaw] ADD  CONSTRAINT [DF_SalesRaw_LoadDttm]  DEFAULT (sysdatetime()) FOR [LoadDttm]
GO


----Store Master loaded from Store_master.csv----

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg].[StoreMaster](
	[StoreId] [varchar](50) NOT NULL,
	[StoreName] [varchar](200) NULL,
	[City] [varchar](100) NULL,
	[State] [varchar](50) NULL,
	[Region] [varchar](50) NULL,
	[LoadDttm] [datetime2](3) NOT NULL
) ON [PRIMARY]
GO

ALTER TABLE [stg].[StoreMaster] ADD  DEFAULT (sysutcdatetime()) FOR [LoadDttm]
GO


