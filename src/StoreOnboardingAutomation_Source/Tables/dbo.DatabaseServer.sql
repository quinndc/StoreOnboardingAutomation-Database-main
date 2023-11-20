USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[DatabaseServer]    Script Date: 7/7/2022 9:43:04 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DatabaseServer]
(
[DatabaseServerId] [int] NOT NULL IDENTITY(1, 1),
[ServerName] [varchar] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[IpAddress] [varchar] (15) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[IsDeleted] [bit] NOT NULL,
[DateCreated] [datetime] NULL,
[CreatedBy] [int] NULL,
[DateModified] [datetime] NULL,
[ModifiedBy] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DatabaseServer] ADD CONSTRAINT [PK_DatabaseServer] PRIMARY KEY CLUSTERED ([DatabaseServerId]) ON [PRIMARY]
GO
ALTER TABLE [dbo].[DatabaseServer] ADD CONSTRAINT [FK_DatabaseServer_CreatedBy] FOREIGN KEY ([CreatedBy]) REFERENCES [Security].[User] ([UserId])
GO
ALTER TABLE [dbo].[DatabaseServer] ADD CONSTRAINT [FK_DatabaseServer_ModifiedBy] FOREIGN KEY ([ModifiedBy]) REFERENCES [Security].[User] ([UserId])
GO
