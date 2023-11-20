USE [StoreOnboardingAutomation]
GO

/****** Object:  Table [dbo].[CrmImportFileHistoryBadRow]    Script Date: 7/7/2022 9:41:17 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CrmImportFileHistoryBadRow](
	[CrmImportFileHistoryBadRowId] [int] IDENTITY(1,1) NOT NULL,
	[CrmImportFileHistoryId] [int] NOT NULL,
	[LineNumber] [int] NOT NULL,
	[RowNumber] [int] NOT NULL,
	[RowContent] [varchar](max) NOT NULL,
	[ExceptionMessage] [varchar](500) NULL,
 CONSTRAINT [PK_CrmImportFileHistoryBadRow] PRIMARY KEY CLUSTERED 
(
	[CrmImportFileHistoryBadRowId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[CrmImportFileHistoryBadRow]  WITH CHECK ADD  CONSTRAINT [FK_CrmImportFileHistoryBadRow_CrmImportFileHistory] FOREIGN KEY([CrmImportFileHistoryId])
REFERENCES [dbo].[CrmImportFileHistory] ([CrmImportFileHistoryId])
GO

ALTER TABLE [dbo].[CrmImportFileHistoryBadRow] CHECK CONSTRAINT [FK_CrmImportFileHistoryBadRow_CrmImportFileHistory]
GO


