CREATE TABLE [Security].[RoleClaim]
(
[RoleClaimId] [int] NOT NULL IDENTITY(1, 1),
[RoleId] [int] NOT NULL,
[ClaimType] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[ClaimValue] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY]
GO
ALTER TABLE [Security].[RoleClaim] ADD CONSTRAINT [PK_RoleClaim] PRIMARY KEY CLUSTERED ([RoleClaimId]) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [IX_RoleClaim_RoleId] ON [Security].[RoleClaim] ([RoleId]) ON [PRIMARY]
GO
ALTER TABLE [Security].[RoleClaim] ADD CONSTRAINT [FK_RoleClaim_Role_RoleId] FOREIGN KEY ([RoleId]) REFERENCES [Security].[Role] ([RoleId]) ON DELETE CASCADE
GO
