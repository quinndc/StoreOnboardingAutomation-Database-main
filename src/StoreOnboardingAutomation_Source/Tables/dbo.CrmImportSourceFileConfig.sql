USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[CrmImportSourceFileConfig]    Script Date: 7/7/2022 9:42:13 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CrmImportSourceFileConfig]
(
[CrmImportSourceFileConfigId] [int] NOT NULL IDENTITY(1, 1),
[CrmImportSourceConfigId] [int] NOT NULL,
[FileNameRegexPattern] [varchar] (250) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[OutputTableNameStartsWith] [varchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[Delimiter] [varchar] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[IsDeleted] [bit] NOT NULL,
[DateCreated] [datetime] NULL,
[CreatedBy] [int] NULL,
[DateModified] [datetime] NULL,
[ModifiedBy] [int] NULL,
[IsRequired] [bit] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CrmImportSourceFileConfig] ADD CONSTRAINT [PK_CrmImportSourceFile] PRIMARY KEY CLUSTERED ([CrmImportSourceFileConfigId]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CrmImportSourceFileConfig] ADD CONSTRAINT [FK_CrmImportSourceFileConfig_CreatedBy] FOREIGN KEY ([CreatedBy]) REFERENCES [Security].[User] ([UserId])
GO
ALTER TABLE [dbo].[CrmImportSourceFileConfig] ADD CONSTRAINT [FK_CrmImportSourceFileConfig_CrmImportSourceConfig] FOREIGN KEY ([CrmImportSourceConfigId]) REFERENCES [dbo].[CrmImportSourceConfig] ([CrmImportSourceConfigId])
GO
ALTER TABLE [dbo].[CrmImportSourceFileConfig] ADD CONSTRAINT [FK_CrmImportSourceFileConfig_ModifiedBy] FOREIGN KEY ([ModifiedBy]) REFERENCES [Security].[User] ([UserId])
GO

ALTER TABLE [dbo].[CrmImportSourceFileConfig] CHECK CONSTRAINT [FK_CrmImportSourceFileConfig_CrmImportSourceConfig]
GO
