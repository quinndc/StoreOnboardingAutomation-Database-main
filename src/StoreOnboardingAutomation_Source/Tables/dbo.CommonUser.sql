CREATE TABLE [dbo].[CommonUser](
	[PKCommonUserId] [int] IDENTITY(1,1) NOT NULL,
	[CRMUserID] [varchar](50) NULL,
	[UserDateCreated] [datetime] NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[FKUserTypeID] [int] NOT NULL,
	[FirstName] [varchar](32) NULL,
	[LastName] [varchar](32) NULL,
	[DateModified] [datetime] NULL,
	[BDCUser] [bit] NULL,
 CONSTRAINT [PK_CommonUser] PRIMARY KEY CLUSTERED 
(
	[PKCommonUserId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[CommonUser]  WITH CHECK ADD  CONSTRAINT [FK_CommonUser_UserTypeID] FOREIGN KEY([FKUserTypeID])
REFERENCES [dbo].[UserType] ([PKUserTypeId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[CommonUser] CHECK CONSTRAINT [FK_CommonUser_UserTypeID]
GO

CREATE NONCLUSTERED INDEX [ix_CommonUser_FKImportLogID] ON [dbo].[CommonUser] ([FKImportLogID])
GO