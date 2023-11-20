CREATE TABLE [Security].[UserRole]
(
[UserId] [int] NOT NULL,
[RoleId] [int] NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [Security].[UserRole] ADD CONSTRAINT [PK_UserRole] PRIMARY KEY CLUSTERED ([UserId], [RoleId]) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [IX_UserRole_RoleId] ON [Security].[UserRole] ([RoleId]) ON [PRIMARY]
GO
ALTER TABLE [Security].[UserRole] ADD CONSTRAINT [FK_UserRole_Role_RoleId] FOREIGN KEY ([RoleId]) REFERENCES [Security].[Role] ([RoleId]) ON DELETE CASCADE
GO
ALTER TABLE [Security].[UserRole] ADD CONSTRAINT [FK_UserRole_User_UserId] FOREIGN KEY ([UserId]) REFERENCES [Security].[User] ([UserId]) ON DELETE CASCADE
GO
