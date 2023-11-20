-- <Migration ID="4feee107-8781-4a2d-a4d7-8637b5fcdea5" />
GO

PRINT N'Creating [dbo].[CrmImportHistory]'
GO
IF OBJECT_ID(N'[dbo].[CrmImportHistory]', 'U') IS NULL
CREATE TABLE [dbo].[CrmImportHistory]
(
[CrmImportHistoryId] [int] NOT NULL IDENTITY(1, 1),
[CrmImportSourceConfigId] [int] NOT NULL,
[StartedOn] [datetime] NULL,
[EndedOn] [datetime] NULL,
[Status] [varchar] (50) NOT NULL,
[ImportName] [varchar] (100) NULL,
[CrmSourceName] [varchar] (50) NOT NULL,
[FolderPath] [varchar] (500) NOT NULL,
[GalaxyDriveServerId] [int] NOT NULL,
[GalaxyStoreId] [int] NOT NULL,
[DatabaseHost] [varchar] (100) NOT NULL,
[DatabaseName] [varchar] (100) NOT NULL
)
GO
PRINT N'Creating primary key [PK_CrmImportHistory] on [dbo].[CrmImportHistory]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PK_CrmImportHistory]', 'PK') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportHistory]', 'U'))
ALTER TABLE [dbo].[CrmImportHistory] ADD CONSTRAINT [PK_CrmImportHistory] PRIMARY KEY CLUSTERED ([CrmImportHistoryId])
GO
PRINT N'Creating [dbo].[CrmImportFileHistory]'
GO
IF OBJECT_ID(N'[dbo].[CrmImportFileHistory]', 'U') IS NULL
CREATE TABLE [dbo].[CrmImportFileHistory]
(
[CrmImportFileHistoryId] [int] NOT NULL IDENTITY(1, 1),
[CrmImportHistoryId] [int] NOT NULL,
[CrmImportSourceFileConfigId] [int] NOT NULL,
[StartedOn] [datetime] NULL,
[EndedOn] [datetime] NULL,
[Status] [varchar] (50) NOT NULL,
[InputFileNameRegexPattern] [varchar] (250) NOT NULL,
[InputFileName] [varchar] (250) NULL,
[OutputTableName] [varchar] (100) NULL,
[GoodRowCount] [int] NULL,
[BadRowCount] [int] NULL,
[ExceptionDetails] [varchar] (max) NULL
)
GO
PRINT N'Creating primary key [PK_CrmImportFileHistory] on [dbo].[CrmImportFileHistory]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PK_CrmImportFileHistory]', 'PK') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportFileHistory]', 'U'))
ALTER TABLE [dbo].[CrmImportFileHistory] ADD CONSTRAINT [PK_CrmImportFileHistory] PRIMARY KEY CLUSTERED ([CrmImportFileHistoryId])
GO
PRINT N'Creating [dbo].[CrmImportSourceFileConfig]'
GO
IF OBJECT_ID(N'[dbo].[CrmImportSourceFileConfig]', 'U') IS NULL
CREATE TABLE [dbo].[CrmImportSourceFileConfig]
(
[CrmImportSourceFileConfigId] [int] NOT NULL IDENTITY(1, 1),
[CrmImportSourceConfigId] [int] NOT NULL,
[FileNameRegexPattern] [varchar] (250) NOT NULL,
[OutputTableNameStartsWith] [varchar] (100) NOT NULL,
[Delimiter] [varchar] (10) NULL,
[IsDeleted] [bit] NOT NULL
)
GO
PRINT N'Creating primary key [PK_CrmImportSourceFile] on [dbo].[CrmImportSourceFileConfig]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PK_CrmImportSourceFile]', 'PK') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportSourceFileConfig]', 'U'))
ALTER TABLE [dbo].[CrmImportSourceFileConfig] ADD CONSTRAINT [PK_CrmImportSourceFile] PRIMARY KEY CLUSTERED ([CrmImportSourceFileConfigId])
GO
PRINT N'Creating [dbo].[CrmImportFileHistoryBadRow]'
GO
IF OBJECT_ID(N'[dbo].[CrmImportFileHistoryBadRow]', 'U') IS NULL
CREATE TABLE [dbo].[CrmImportFileHistoryBadRow]
(
[CrmImportFileHistoryBadRowId] [int] NOT NULL IDENTITY(1, 1),
[CrmImportFileHistoryId] [int] NOT NULL,
[LineNumber] [int] NOT NULL,
[RowNumber] [int] NOT NULL,
[RowContent] [varchar] (max) NOT NULL,
[ExceptionMessage] [varchar] (500) NULL
)
GO
PRINT N'Creating primary key [PK_CrmImportFileHistoryBadRow] on [dbo].[CrmImportFileHistoryBadRow]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PK_CrmImportFileHistoryBadRow]', 'PK') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportFileHistoryBadRow]', 'U'))
ALTER TABLE [dbo].[CrmImportFileHistoryBadRow] ADD CONSTRAINT [PK_CrmImportFileHistoryBadRow] PRIMARY KEY CLUSTERED ([CrmImportFileHistoryBadRowId])
GO
PRINT N'Creating [dbo].[CrmImportSourceConfig]'
GO
IF OBJECT_ID(N'[dbo].[CrmImportSourceConfig]', 'U') IS NULL
CREATE TABLE [dbo].[CrmImportSourceConfig]
(
[CrmImportSourceConfigId] [int] NOT NULL IDENTITY(1, 1),
[Name] [varchar] (50) NOT NULL,
[IsDeleted] [bit] NOT NULL
)
GO
PRINT N'Creating primary key [PK_CrmImportSourceConfig] on [dbo].[CrmImportSourceConfig]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PK_CrmImportSourceConfig]', 'PK') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportSourceConfig]', 'U'))
ALTER TABLE [dbo].[CrmImportSourceConfig] ADD CONSTRAINT [PK_CrmImportSourceConfig] PRIMARY KEY CLUSTERED ([CrmImportSourceConfigId])
GO
PRINT N'Adding foreign keys to [dbo].[CrmImportFileHistoryBadRow]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CrmImportFileHistoryBadRow_CrmImportFileHistory]','F') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportFileHistoryBadRow]', 'U'))
ALTER TABLE [dbo].[CrmImportFileHistoryBadRow] ADD CONSTRAINT [FK_CrmImportFileHistoryBadRow_CrmImportFileHistory] FOREIGN KEY ([CrmImportFileHistoryId]) REFERENCES [dbo].[CrmImportFileHistory] ([CrmImportFileHistoryId])
GO
PRINT N'Adding foreign keys to [dbo].[CrmImportFileHistory]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CrmImportFileHistory_CrmImportHistory]','F') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportFileHistory]', 'U'))
ALTER TABLE [dbo].[CrmImportFileHistory] ADD CONSTRAINT [FK_CrmImportFileHistory_CrmImportHistory] FOREIGN KEY ([CrmImportHistoryId]) REFERENCES [dbo].[CrmImportHistory] ([CrmImportHistoryId])
GO
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CrmImportFileHistory_CrmImportSourceFileConfig]','F') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportFileHistory]', 'U'))
ALTER TABLE [dbo].[CrmImportFileHistory] ADD CONSTRAINT [FK_CrmImportFileHistory_CrmImportSourceFileConfig] FOREIGN KEY ([CrmImportSourceFileConfigId]) REFERENCES [dbo].[CrmImportSourceFileConfig] ([CrmImportSourceFileConfigId])
GO
PRINT N'Adding foreign keys to [dbo].[CrmImportHistory]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CrmImportHistory_CrmImportSourceConfig]','F') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportHistory]', 'U'))
ALTER TABLE [dbo].[CrmImportHistory] ADD CONSTRAINT [FK_CrmImportHistory_CrmImportSourceConfig] FOREIGN KEY ([CrmImportSourceConfigId]) REFERENCES [dbo].[CrmImportSourceConfig] ([CrmImportSourceConfigId])
GO
PRINT N'Adding foreign keys to [dbo].[CrmImportSourceFileConfig]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE object_id = OBJECT_ID(N'[dbo].[FK_CrmImportSourceFileConfig_CrmImportSourceConfig]','F') AND parent_object_id = OBJECT_ID(N'[dbo].[CrmImportSourceFileConfig]', 'U'))
ALTER TABLE [dbo].[CrmImportSourceFileConfig] ADD CONSTRAINT [FK_CrmImportSourceFileConfig_CrmImportSourceConfig] FOREIGN KEY ([CrmImportSourceConfigId]) REFERENCES [dbo].[CrmImportSourceConfig] ([CrmImportSourceConfigId])
GO
