CREATE TABLE [dbo].[CommonCustomer](
	[PKCommonCustomerId] [int] IDENTITY(1,1) NOT NULL,
	[CRMCustomerID] [varchar](50) NULL,
	[CustomerDateCreated] [datetime] NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[Sales1UserID] [varchar](50) NULL,
	[Sales2UserID] [varchar](50) NULL,
	[BDCUserID] [varchar](50) NULL,
	[BuyerType] [int] NOT NULL,
	[PrimaryFirstName] [varchar](32) NULL,
	[PrimaryLastName] [varchar](32) NULL,
	[Email] [varchar](128) NULL,
	[CellPhone] [varchar](16) NULL,
	[PrimaryDOB] [datetime] NULL,
	[Address1] [varchar](128) NULL,
	[Address2] [varchar](128) NULL,
	[City] [varchar](32) NULL,
	[State] [varchar](2) NULL,
	[Zip] [varchar](10) NULL,
	[CustomerType] [int] NOT NULL,
	[CompanyName] [varchar](128) NULL,
	[HomePhone] [varchar](10) NULL,
	[WorkPhone] [varchar](10) NULL,
	[PrimaryMiddleName] [varchar](256) NULL,
	[CoFirstName] [varchar](32) NULL,
	[CoLastName] [varchar](32) NULL,
	[CoDOB] [datetime] NULL,
	[CoEmail] [varchar](64) NULL,
	[DateModified] [datetime] NULL,
	[DoNotCall] [bit] NULL,
	[DoNotEmail] [bit] NULL,
	[DoNotMail] [bit] NULL,
 CONSTRAINT [PK_CommonCustomer] PRIMARY KEY CLUSTERED 
(
	[PKCommonCustomerId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [ix_CommonCustomer_FKImportLogID] ON [dbo].[CommonCustomer] ([FKImportLogID])
GO

