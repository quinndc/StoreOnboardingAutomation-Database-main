CREATE TABLE [dbo].[CommonDealLog](
	[PKCommonDealLogId] [int] IDENTITY(1,1) NOT NULL,
	[FKCustomerID] [varchar](50) NULL,
	[DealLogDateCreated] [datetime] NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[FKUserIDSales1] [varchar](50) NULL,
	[FKDealID] [varchar](50) NULL,
	[DealLogType] [int] NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [PK_CommonDealLog] PRIMARY KEY CLUSTERED 
(
	[PKCommonDealLogId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ix_CommonDealLog_FKImportLogID_FKCustomerID_Includes] ON [dbo].[CommonDealLog] ([FKImportLogID], [FKCustomerID]) INCLUDE ([FKDealID], [FKUserIDSales1], [DealLogType])
GO