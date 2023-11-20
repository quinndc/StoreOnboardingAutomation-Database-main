CREATE TABLE [Security].[UserLogin]
(
[LoginProvider] [nvarchar] (450) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[ProviderKey] [nvarchar] (450) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[ProviderDisplayName] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[UserId] [int] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [Security].[UserLogin] ADD CONSTRAINT [PK_UserLogin] PRIMARY KEY CLUSTERED ([LoginProvider], [ProviderKey]) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [IX_UserLogin_UserId] ON [Security].[UserLogin] ([UserId]) ON [PRIMARY]
GO
ALTER TABLE [Security].[UserLogin] ADD CONSTRAINT [FK_UserLogin_User_UserId] FOREIGN KEY ([UserId]) REFERENCES [Security].[User] ([UserId]) ON DELETE CASCADE
GO
