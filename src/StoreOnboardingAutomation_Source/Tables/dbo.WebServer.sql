USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[WebServer]    Script Date: 7/7/2022 9:45:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[WebServer]
(
[WebServerId] [int] NOT NULL IDENTITY(1, 1),
[RootUrl] [varchar] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[IsDeleted] [bit] NOT NULL,
[DateCreated] [datetime] NULL,
[CreatedBy] [int] NULL,
[DateModified] [datetime] NULL,
[ModifiedBy] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[WebServer] ADD CONSTRAINT [PK_WebServer] PRIMARY KEY CLUSTERED ([WebServerId]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[WebServer] ADD CONSTRAINT [FK_WebServer_CreatedBy] FOREIGN KEY ([CreatedBy]) REFERENCES [Security].[User] ([UserId])
GO
ALTER TABLE [dbo].[WebServer] ADD CONSTRAINT [FK_WebServer_ModifiedBy] FOREIGN KEY ([ModifiedBy]) REFERENCES [Security].[User] ([UserId])
GO
