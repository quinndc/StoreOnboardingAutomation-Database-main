CREATE TABLE [dbo].[CommonDeal](
	[PKCommonDealId] [int] IDENTITY(1,1) NOT NULL,
	[CRMDealID] [varchar](50) NULL,
	[DealDateCreated] [datetime] NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[FKBuyerID] [varchar](50) NULL,
	[FKCoBuyerID] [varchar](50) NULL,
	[FKUserIDSales1] [varchar](50) NULL,
	[FKUserIDSales2] [varchar](50) NULL,
	[FKuserIDBDC] [varchar](50) NULL,
	[FKCustomerID] [varchar](50) NULL,
	[Delivered] [bit] NULL,
	[SoldDate] [datetime] NULL,
	[SourceType] [int] NULL,
	[SourceDescription] [varchar](256) NULL,
	[ProposalDealFlag] [datetime] NULL,
	[InactiveDealFlag] [datetime] NULL,
	[DateModified] [datetime] NULL,
	[SoldNotDeliveredFlag] [bit] NULL,
	[OrderedFlag] [bit] NULL,
	[PendingFlag] [bit] NULL,
	[DeadFlag] [bit] NULL,
	[ServiceFlag] [bit] NULL,
	[SoldFlag] [bit] NULL,
 CONSTRAINT [PK_CommonDeal] PRIMARY KEY CLUSTERED 
(
	[PKCommonDealId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ix_CommonDeal_FKImportLogID_FKCustomerID_Includes] ON [dbo].[CommonDeal] ([FKImportLogID], [FKCustomerID]) INCLUDE ([fkUserIDSales1], [fkUserIDSales2], [fkUserIDBDC])
GO

CREATE NONCLUSTERED INDEX [ix_CommonDeal_SoldFlag_Includes] ON [dbo].[CommonDeal] ([SoldFlag]) INCLUDE ([CRMDealID],[FKImportLogID])
GO