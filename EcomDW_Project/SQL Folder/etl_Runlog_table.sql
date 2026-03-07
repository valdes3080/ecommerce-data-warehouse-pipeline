USE [EcomDW]
GO

---Create etl.Runlog table----


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [etl].[RunLog](
	[RunLogId] [bigint] IDENTITY(1,1) NOT NULL,
	[RunId] [uniqueidentifier] NOT NULL,
	[StepName] [nvarchar](100) NOT NULL,
	[RowCount] [int] NULL,
	[Status] [nvarchar](20) NOT NULL,
	[SourceFile] [nvarchar](260) NULL,
	[LoggedAt] [datetime2](0) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[RunLogId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [etl].[RunLog] ADD  DEFAULT (sysdatetime()) FOR [LoggedAt]
GO

ALTER TABLE [etl].[RunLog]  WITH CHECK ADD  CONSTRAINT [CK_RunLog_Status] CHECK  (([Status]='Failed' OR [Status]='Succeeded' OR [Status]='Started'))
GO

ALTER TABLE [etl].[RunLog] CHECK CONSTRAINT [CK_RunLog_Status]
GO


