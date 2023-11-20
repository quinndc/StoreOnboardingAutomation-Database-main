CREATE TABLE [dbo].[CommonTask](
	[PKCommonTaskId] [int] IDENTITY(1,1) NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[FKCustomerID] [varchar](50) NULL,
	[FKUserID] [varchar](50) NULL,
	[FKCreatedByUserID] [varchar](50) NULL,
	[FKCompletedByUserID] [varchar](50) NULL,
	[FKDealID] [varchar](50) NULL,
	[TaskDateCreated] [datetime] NOT NULL,
	[TaskDateModified] [datetime] NOT NULL,
	[TaskDateCompleted] [datetime] NOT NULL,
	[Subject] [varchar](2048) NULL,
	[Description] [varchar](8000) NULL,
	[Resolution] [varchar](8000) NULL,
	[DateStart] [datetime] NULL,
	[DateDue] [datetime] NULL,
	[ResultType] [int] NOT NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [PK_CommonTask] PRIMARY KEY CLUSTERED 
(
	[PKCommonTaskId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[CommonTask] ADD  DEFAULT ('') FOR [Description]
GO

CREATE NONCLUSTERED INDEX [ix_CommonTask_FKImportLogID_FKCustomerID_Includes] ON [dbo].[CommonTask] ([FKImportLogID], [FKCustomerID]) INCLUDE ([FKDealID], [fkUserID], [fkCreatedByUserID], [fkCompletedByUserID])
GO