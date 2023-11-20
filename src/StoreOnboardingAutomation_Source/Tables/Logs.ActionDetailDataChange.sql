CREATE TABLE [Logs].[ActionDetailDataChange]
(
[ActionDetailDataChangeId] [int] NOT NULL IDENTITY(1, 1),
[ActionOverviewID] [int] NOT NULL,
[EntityType] [varchar] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[EntityId] [int] NOT NULL,
[ActionType] [varchar] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [Logs].[ActionDetailDataChange] ADD CONSTRAINT [PK_DataChangeHistory] PRIMARY KEY CLUSTERED ([ActionDetailDataChangeId]) ON [PRIMARY]
GO
ALTER TABLE [Logs].[ActionDetailDataChange] ADD CONSTRAINT [FK_DataChangeHistory_ActionHistory] FOREIGN KEY ([ActionOverviewID]) REFERENCES [Logs].[ActionOverview] ([ActionOverviewId])
GO
