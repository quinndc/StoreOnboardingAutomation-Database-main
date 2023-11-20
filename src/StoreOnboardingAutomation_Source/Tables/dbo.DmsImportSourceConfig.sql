USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[DmsImportSourceConfig]    Script Date: 7/7/2022 9:44:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DmsImportSourceConfig]
(
[DmsImportSourceConfigId] [int] NOT NULL IDENTITY(1, 1),
[Name] [varchar] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[IsDeleted] [bit] NOT NULL,
[DateCreated] [datetime] NULL,
[CreatedBy] [int] NULL,
[DateModified] [datetime] NULL,
[ModifiedBy] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DmsImportSourceConfig] ADD CONSTRAINT [PK_DmsImportSourceType] PRIMARY KEY CLUSTERED ([DmsImportSourceConfigId]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DmsImportSourceConfig] ADD CONSTRAINT [FK_DmsImportSourceConfig_CreatedBy] FOREIGN KEY ([CreatedBy]) REFERENCES [Security].[User] ([UserId])
GO
ALTER TABLE [dbo].[DmsImportSourceConfig] ADD CONSTRAINT [FK_DmsImportSourceConfig_ModifiedBy] FOREIGN KEY ([ModifiedBy]) REFERENCES [Security].[User] ([UserId])
GO
