CREATE TABLE [dbo].[ImportChildStage](
	[pkImportChildStageID] [int] IDENTITY(1,1) NOT NULL,
	[fkImportParentStageID] [int] NULL,
	[ChildStageName] [varchar](100) NULL,
PRIMARY KEY CLUSTERED 
(
	[pkImportChildStageID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ImportChildStage]  WITH CHECK ADD  CONSTRAINT [FK_ImportChildStage_ImportParentStage] FOREIGN KEY([fkImportParentStageID])
REFERENCES [dbo].[ImportParentStage] ([pkImportParentStageID])
GO

ALTER TABLE [dbo].[ImportChildStage] CHECK CONSTRAINT [FK_ImportChildStage_ImportParentStage]
GO

