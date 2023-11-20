CREATE TABLE [dbo].[CommonCustomerLog](
	[PKCommonCustomerLogId] [int] IDENTITY(1,1) NOT NULL,
	[FKCustomerID] [varchar](50) NULL,
	[CustomerLogDateCreated] [datetime] NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[FKUserIDSales1] [varchar](50) NULL,
	[FKDealID] [varchar](50) NULL,
	[Notes] [varchar](4000) NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [PK_CommonCustomerLog] PRIMARY KEY CLUSTERED 
(
	[PKCommonCustomerLogId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ix_CommonCustomerLog_FKImportLogID_FKCustomerID_Includes] ON [dbo].[CommonCustomerLog] ([FKImportLogID], [FKCustomerID]) INCLUDE ([FKDealID], [FKUserIDSales1])
GO