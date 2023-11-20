CREATE TABLE [Logs].[ActionOverview]
(
[ActionOverviewId] [int] NOT NULL IDENTITY(1, 1),
[Source] [varchar] (250) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[Action] [varchar] (250) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[CreatedOn] [datetime] NULL,
[CreatedBy] [int] NULL,
[StoreGroupId] [int] NULL,
[StoreId] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [Logs].[ActionOverview] ADD CONSTRAINT [PK_ActionHistory] PRIMARY KEY CLUSTERED ([ActionOverviewId]) ON [PRIMARY]
GO
ALTER TABLE [Logs].[ActionOverview] ADD CONSTRAINT [FK_ActionHistory_User] FOREIGN KEY ([CreatedBy]) REFERENCES [Security].[User] ([UserId])
GO
