CREATE TABLE [Security].[Role]
(
[RoleId] [int] NOT NULL IDENTITY(1, 1),
[Name] [nvarchar] (256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[NormalizedName] [nvarchar] (256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[ConcurrencyStamp] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY]
GO
ALTER TABLE [Security].[Role] ADD CONSTRAINT [PK_Role] PRIMARY KEY CLUSTERED ([RoleId]) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [RoleNameIndex] ON [Security].[Role] ([NormalizedName]) WHERE ([NormalizedName] IS NOT NULL) ON [PRIMARY]
GO
