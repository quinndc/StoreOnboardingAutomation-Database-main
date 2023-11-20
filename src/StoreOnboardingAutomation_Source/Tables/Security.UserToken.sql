CREATE TABLE [Security].[UserToken]
(
[UserId] [int] NOT NULL,
[LoginProvider] [nvarchar] (450) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[Name] [nvarchar] (450) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[Value] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY]
GO
ALTER TABLE [Security].[UserToken] ADD CONSTRAINT [PK_UserToken] PRIMARY KEY CLUSTERED ([UserId], [LoginProvider], [Name]) ON [PRIMARY]
GO
ALTER TABLE [Security].[UserToken] ADD CONSTRAINT [FK_UserToken_User_UserId] FOREIGN KEY ([UserId]) REFERENCES [Security].[User] ([UserId]) ON DELETE CASCADE
GO
