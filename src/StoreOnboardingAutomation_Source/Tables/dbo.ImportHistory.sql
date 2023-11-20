CREATE TABLE [dbo].[ImportHistory](
	[pkImportHistoryID] [int] IDENTITY(1,1) NOT NULL,
	[fkImportID] [int] NOT NULL,
	[fkImportParentStageID] [int] NOT NULL,
	[fkImportChildStageID] [int] NOT NULL,
	[StageStart] [datetime] NULL,
	[StageEnd] [datetime] NULL,
	[RowCount] [int] NULL,
	[TableName] [varchar(100)] NULL,
	[TableStartCount] [int] NULL
 CONSTRAINT [PK_ImportHistory] PRIMARY KEY CLUSTERED 
(
	[pkImportHistoryID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ImportHistory] ADD  DEFAULT (getdate()) FOR [StageStart]
GO

ALTER TABLE [dbo].[ImportHistory]  WITH CHECK ADD  CONSTRAINT [FK_ImportHistory_ImportChildStage] FOREIGN KEY([fkImportChildStageID])
REFERENCES [dbo].[ImportChildStage] ([pkImportChildStageID])
GO

ALTER TABLE [dbo].[ImportHistory] CHECK CONSTRAINT [FK_ImportHistory_ImportChildStage]
GO

ALTER TABLE [dbo].[ImportHistory]  WITH CHECK ADD  CONSTRAINT [FK_ImportHistory_ImportLog] FOREIGN KEY([fkImportID])
REFERENCES [dbo].[ImportLog] ([pkImportID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[ImportHistory] CHECK CONSTRAINT [FK_ImportHistory_ImportLog]
GO

ALTER TABLE [dbo].[ImportHistory]  WITH CHECK ADD  CONSTRAINT [FK_ImportHistory_ImportParentStage] FOREIGN KEY([fkImportParentStageID])
REFERENCES [dbo].[ImportParentStage] ([pkImportParentStageID])
GO

ALTER TABLE [dbo].[ImportHistory] CHECK CONSTRAINT [FK_ImportHistory_ImportParentStage]
GO


