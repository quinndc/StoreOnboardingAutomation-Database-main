CREATE TABLE [Logs].[ActionDetailText]
(
[ActionDetailTextId] [int] NOT NULL IDENTITY(1, 1),
[ActionOverviewId] [int] NOT NULL,
[CreatedOn] [datetime] NOT NULL,
[LogLevel] [varchar] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[Message] [varchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [Logs].[ActionDetailText] ADD CONSTRAINT [PK_ActionEventHistory] PRIMARY KEY CLUSTERED ([ActionDetailTextId]) ON [PRIMARY]
GO
ALTER TABLE [Logs].[ActionDetailText] ADD CONSTRAINT [FK_ActionEventHistory_ActionHistory] FOREIGN KEY ([ActionOverviewId]) REFERENCES [Logs].[ActionOverview] ([ActionOverviewId]) ON DELETE CASCADE
GO
