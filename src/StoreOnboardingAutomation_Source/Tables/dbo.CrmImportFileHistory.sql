USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[CrmImportFileHistory]    Script Date: 7/7/2022 9:40:47 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CrmImportFileHistory](
	[CrmImportFileHistoryId] [int] IDENTITY(1,1) NOT NULL,
	[CrmImportHistoryId] [int] NOT NULL,
	[CrmImportSourceFileConfigId] [int] NOT NULL,
	[StartedOn] [datetime] NULL,
	[EndedOn] [datetime] NULL,
	[Status] [varchar](50) NOT NULL,
	[InputFileNameRegexPattern] [varchar](250) NOT NULL,
	[InputFileName] [varchar](250) NULL,
	[OutputTableName] [varchar](100) NULL,
	[GoodRowCount] [int] NULL,
	[BadRowCount] [int] NULL,
	[ExceptionDetails] [varchar](max) NULL,
 CONSTRAINT [PK_CrmImportFileHistory] PRIMARY KEY CLUSTERED 
(
	[CrmImportFileHistoryId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[CrmImportFileHistory]  WITH CHECK ADD  CONSTRAINT [FK_CrmImportFileHistory_CrmImportHistory] FOREIGN KEY([CrmImportHistoryId])
REFERENCES [dbo].[CrmImportHistory] ([CrmImportHistoryId])
GO

ALTER TABLE [dbo].[CrmImportFileHistory] CHECK CONSTRAINT [FK_CrmImportFileHistory_CrmImportHistory]
GO

ALTER TABLE [dbo].[CrmImportFileHistory]  WITH CHECK ADD  CONSTRAINT [FK_CrmImportFileHistory_CrmImportSourceFileConfig] FOREIGN KEY([CrmImportSourceFileConfigId])
REFERENCES [dbo].[CrmImportSourceFileConfig] ([CrmImportSourceFileConfigId])
GO

ALTER TABLE [dbo].[CrmImportFileHistory] CHECK CONSTRAINT [FK_CrmImportFileHistory_CrmImportSourceFileConfig]
GO


