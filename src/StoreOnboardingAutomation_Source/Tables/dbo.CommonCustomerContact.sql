CREATE TABLE [dbo].[CommonCustomerContact](
	[PKCommonCustomerContactId] [int] IDENTITY(1,1) NOT NULL,
	[FKCustomerID] [varchar](50) NULL,
	[CustomerContactDateCreated] [datetime] NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[ContactLabelType] [int] NULL,
	[CommunicationType] [int] NULL,
	[Value] [varchar](320) NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [PK_CommonCustomerContact] PRIMARY KEY CLUSTERED 
(
	[PKCommonCustomerContactId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ix_CommonCustomerContact_FKImportLogID_FKCustomerID_Includes] ON [dbo].[CommonCustomerContact] ([FKImportLogID], [FKCustomerID]) INCLUDE ([CommunicationType])
GO