USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[StoreGroup]    Script Date: 7/7/2022 9:45:23 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[StoreGroup]
(
[StoreGroupId] [int] NOT NULL IDENTITY(1, 1),
[Name] [varchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[GalaxyDriveServerId] [int] NULL,
[DatabaseServerId] [int] NULL,
[DatabaseName] [varchar] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[WebServerId] [int] NULL,
[IsDeleted] [bit] NOT NULL,
[DateCreated] [datetime] NULL,
[CreatedBy] [int] NULL,
[DateModified] [datetime] NOT NULL,
[ModifiedBy] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[StoreGroup] ADD CONSTRAINT [PK_StoreGroup] PRIMARY KEY CLUSTERED ([StoreGroupId]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[StoreGroup] ADD CONSTRAINT [FK_StoreGroup_CreatedBy] FOREIGN KEY ([CreatedBy]) REFERENCES [Security].[User] ([UserId])
GO
ALTER TABLE [dbo].[StoreGroup] ADD CONSTRAINT [FK_StoreGroup_GalaxyDriveServerId] FOREIGN KEY ([StoreGroupId]) REFERENCES [dbo].[StoreGroup] ([StoreGroupId])
GO
ALTER TABLE [dbo].[StoreGroup] ADD CONSTRAINT [FK_StoreGroup_ModifiedBy] FOREIGN KEY ([ModifiedBy]) REFERENCES [Security].[User] ([UserId])
GO

ALTER TABLE [dbo].[StoreGroup] CHECK CONSTRAINT [FK_StoreGroup_DatabaseServer]
GO

ALTER TABLE [dbo].[StoreGroup] CHECK CONSTRAINT [FK_StoreGroup_GalaxyDriveServerId]
GO

ALTER TABLE [dbo].[StoreGroup] CHECK CONSTRAINT [FK_StoreGroup_WebServer]
GO
