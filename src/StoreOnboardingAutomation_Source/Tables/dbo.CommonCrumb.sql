CREATE TABLE [dbo].[CommonCrumb](
	[PKCommonCrumbId] [int] IDENTITY(1,1) NOT NULL,
	[CrumbDateCreated] [datetime] NOT NULL,
	[CrumbDateModified] [datetime] NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[FKCustomerID] [varchar](50) NULL,
	[FKUserID] [varchar](50) NULL,
	[FKDealID] [varchar](50) NULL,
	[CrumbType] [int] NULL,
	[IsSent] [bit] NULL,
	[IsRead] [bit] NULL,
	[Subject] [varchar](128) NULL,
	[From] [varchar](1024) NULL,
	[To] [varchar](1024) NULL,
	[StrippedMessage] [varchar](8000) NOT NULL,
	[DateRead] [datetime] NULL,
	[UnicodeStrippedMessage] [nvarchar](max) NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [PK_CommonCrumb] PRIMARY KEY CLUSTERED 
(
	[PKCommonCrumbId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ix_CommonCrumb_FKImportLogID_FKCustomerID_Includes] ON [dbo].[CommonCrumb] ([FKImportLogID], [FKCustomerID]) INCLUDE ([FKDealID], [FKUserID], [CrumbType])
GO