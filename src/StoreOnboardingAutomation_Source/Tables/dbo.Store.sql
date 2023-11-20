USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[Store]    Script Date: 7/7/2022 9:45:10 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Store]
(
[StoreId] [int] NOT NULL IDENTITY(1, 1),
[Name] [varchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[StoreGroupId] [int] NOT NULL,
[GalaxyStoreId] [int] NULL,
[CrmImportSourceConfigId] [int] NULL,
[DmsImportSourceConfigId] [int] NULL,
[LaunchDate] [date] NULL,
[IsLive] [bit] NOT NULL,
[IsDeleted] [bit] NOT NULL,
[DateCreated] [datetime] NOT NULL,
[CreatedBy] [int] NULL,
[DateModified] [datetime] NOT NULL,
[ModifiedBy] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Store] ADD CONSTRAINT [PK_Store] PRIMARY KEY CLUSTERED ([StoreId]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Store] ADD CONSTRAINT [FK_Store_CreatedBy] FOREIGN KEY ([CreatedBy]) REFERENCES [Security].[User] ([UserId])
GO
ALTER TABLE [dbo].[Store] ADD CONSTRAINT [FK_Store_CrmImportSourceConfig] FOREIGN KEY ([CrmImportSourceConfigId]) REFERENCES [dbo].[CrmImportSourceConfig] ([CrmImportSourceConfigId]) ON DELETE CASCADE ON UPDATE CASCADE
GO
ALTER TABLE [dbo].[Store] ADD CONSTRAINT [FK_Store_DmsImportSourceConfigId] FOREIGN KEY ([DmsImportSourceConfigId]) REFERENCES [dbo].[DmsImportSourceConfig] ([DmsImportSourceConfigId]) ON DELETE CASCADE ON UPDATE CASCADE
GO
ALTER TABLE [dbo].[Store] ADD CONSTRAINT [FK_Store_ModifiedBy] FOREIGN KEY ([ModifiedBy]) REFERENCES [Security].[User] ([UserId])
GO
ALTER TABLE [dbo].[Store] ADD CONSTRAINT [FK_Store_StoreGroup] FOREIGN KEY ([StoreGroupId]) REFERENCES [dbo].[StoreGroup] ([StoreGroupId]) ON DELETE CASCADE ON UPDATE CASCADE
GO

ALTER TABLE [dbo].[Store] CHECK CONSTRAINT [FK_Store_CrmImportSourceConfig]
GO

ALTER TABLE [dbo].[Store] CHECK CONSTRAINT [FK_Store_DmsImportSourceConfigId]
GO

ALTER TABLE [dbo].[Store] CHECK CONSTRAINT [FK_Store_StoreGroup]
GO
