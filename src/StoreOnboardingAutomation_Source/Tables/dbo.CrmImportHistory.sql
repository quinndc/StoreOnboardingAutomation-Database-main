USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[CrmImportHistory]    Script Date: 7/7/2022 9:41:39 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CrmImportHistory](
	[CrmImportHistoryId] [int] IDENTITY(1,1) NOT NULL,
	[CrmImportSourceConfigId] [int] NOT NULL,
	[StartedOn] [datetime] NULL,
	[EndedOn] [datetime] NULL,
	[Status] [varchar](50) NOT NULL,
	[ImportName] [varchar](100) NULL,
	[CrmSourceName] [varchar](50) NOT NULL,
	[FolderPath] [varchar](500) NOT NULL,
	[GalaxyDriveServerId] [int] NOT NULL,
	[GalaxyStoreId] [int] NOT NULL,
	[DatabaseHost] [varchar](100) NOT NULL,
	[DatabaseName] [varchar](100) NOT NULL,
 CONSTRAINT [PK_CrmImportHistory] PRIMARY KEY CLUSTERED 
(
	[CrmImportHistoryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[CrmImportHistory]  WITH CHECK ADD  CONSTRAINT [FK_CrmImportHistory_CrmImportSourceConfig] FOREIGN KEY([CrmImportSourceConfigId])
REFERENCES [dbo].[CrmImportSourceConfig] ([CrmImportSourceConfigId])
GO

ALTER TABLE [dbo].[CrmImportHistory] CHECK CONSTRAINT [FK_CrmImportHistory_CrmImportSourceConfig]
GO


