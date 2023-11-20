CREATE TABLE [dbo].[ImportLog](
	[pkImportID] [int] IDENTITY(1,1) NOT NULL,
	[GUID] [uniqueidentifier] NULL,
	[ImportType] [int] NULL,
	[DateCreated] [datetime] NULL,
	[FKCRMID] [int] NOT NULL,
	[FKDMSID] [int] NOT NULL,
	[FKStoreID] [int] NOT NULL,
	[DMSCompleted] [bit] NOT NULL,
	[CRMReceived] [bit] NOT NULL,
	[ManualDeliver] [bit] NULL,
	[DealerPhone] [varchar](10) NULL,
	[DateScheduled] [datetime] NULL,
	[DateCompleted] [datetime] NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [PK_ImportLog] PRIMARY KEY CLUSTERED 
(
	[PKImportId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ImportLog] ADD  DEFAULT (newid()) FOR [GUID]
GO

ALTER TABLE [dbo].[ImportLog] ADD  DEFAULT (getdate()) FOR [DateCreated]
GO

ALTER TABLE [dbo].[ImportLog] ADD  DEFAULT ((0)) FOR [DMSCompleted]
GO

ALTER TABLE [dbo].[ImportLog] ADD  DEFAULT ((0)) FOR [CRMReceived]
GO

ALTER TABLE [dbo].[ImportLog] ADD  DEFAULT ((0)) FOR [ManualDeliver]
GO

ALTER TABLE [dbo].[ImportLog]  WITH CHECK ADD  CONSTRAINT [FK_ImportLog_CrmImportSourceConfig] FOREIGN KEY([FKCRMID])
REFERENCES [dbo].[CrmImportSourceConfig] ([CrmImportSourceConfigId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[ImportLog] CHECK CONSTRAINT [FK_ImportLog_CrmImportSourceConfig]
GO

ALTER TABLE [dbo].[ImportLog]  WITH CHECK ADD  CONSTRAINT [FK_ImportLog_DmsImportSourceConfigId] FOREIGN KEY([FKDMSID])
REFERENCES [dbo].[DmsImportSourceConfig] ([DmsImportSourceConfigId])
ON UPDATE CASCADE
ON DELETE CASCADE
GO

ALTER TABLE [dbo].[ImportLog] CHECK CONSTRAINT [FK_ImportLog_DmsImportSourceConfigId]
GO

