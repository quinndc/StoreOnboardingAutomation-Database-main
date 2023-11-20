CREATE TABLE [Security].[UserClaim]
(
[UserClaimId] [int] NOT NULL IDENTITY(1, 1),
[UserId] [int] NOT NULL,
[ClaimType] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[ClaimValue] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY]
GO
ALTER TABLE [Security].[UserClaim] ADD CONSTRAINT [PK_UserClaim] PRIMARY KEY CLUSTERED ([UserClaimId]) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [IX_UserClaim_UserId] ON [Security].[UserClaim] ([UserId]) ON [PRIMARY]
GO
ALTER TABLE [Security].[UserClaim] ADD CONSTRAINT [FK_UserClaim_User_UserId] FOREIGN KEY ([UserId]) REFERENCES [Security].[User] ([UserId]) ON DELETE CASCADE
GO
