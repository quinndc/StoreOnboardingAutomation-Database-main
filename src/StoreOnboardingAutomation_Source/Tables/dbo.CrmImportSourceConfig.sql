USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[CrmImportSourceConfig]    Script Date: 7/7/2022 9:41:57 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CrmImportSourceConfig]
(
[CrmImportSourceConfigId] [int] NOT NULL IDENTITY(1, 1),
[Name] [varchar] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[IsDeleted] [bit] NOT NULL,
[DateCreated] [datetime] NULL,
[CreatedBy] [int] NULL,
[DateModified] [datetime] NULL,
[ModifiedBy] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CrmImportSourceConfig] ADD CONSTRAINT [PK_CrmImportSourceConfig] PRIMARY KEY CLUSTERED ([CrmImportSourceConfigId]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CrmImportSourceConfig] ADD CONSTRAINT [FK_CrmImportSourceConfig_CreatedBy] FOREIGN KEY ([CreatedBy]) REFERENCES [Security].[User] ([UserId])
GO
ALTER TABLE [dbo].[CrmImportSourceConfig] ADD CONSTRAINT [FK_CrmImportSourceConfig_ModifiedBy] FOREIGN KEY ([ModifiedBy]) REFERENCES [Security].[User] ([UserId])
GO
