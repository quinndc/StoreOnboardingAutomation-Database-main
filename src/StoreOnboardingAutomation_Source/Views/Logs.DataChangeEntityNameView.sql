SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

--DROP VIEW dbo.DataChangeEntityNameView

CREATE   VIEW [Logs].[DataChangeEntityNameView] AS 
  (SELECT [ActionDetailDataChangeId], [ActionOverviewID], [Name] EntityName FROM [Logs].[ActionDetailDataChange] JOIN [dbo].[StoreGroup] ON EntityType = 'StoreGroup' AND EntityId = StoreGroupId)
  UNION
  (SELECT [ActionDetailDataChangeId], [ActionOverviewID], [Name] EntityName FROM [Logs].[ActionDetailDataChange] JOIN [dbo].[Store] ON EntityType = 'Store' AND EntityId = StoreId)
  UNION
  (SELECT [ActionDetailDataChangeId], [ActionOverviewID], [ServerName] EntityName FROM [Logs].[ActionDetailDataChange] JOIN [dbo].[DatabaseServer] ON EntityType = 'DatabaseServer' AND EntityId = DatabaseServerId)
  UNION
  (SELECT [ActionDetailDataChangeId], [ActionOverviewID], [Name] EntityName FROM [Logs].[ActionDetailDataChange] JOIN [dbo].[CrmImportSourceConfig] ON EntityType = 'CrmImportSourceConfig' AND EntityId = CrmImportSourceConfigId)
  UNION
  (SELECT [ActionDetailDataChangeId], [ActionOverviewID], [FileNameRegexPattern] EntityName FROM [Logs].[ActionDetailDataChange] JOIN [dbo].[CrmImportSourceFileConfig] ON EntityType = 'CrmImportSourceFileConfig' AND EntityId = CrmImportSourceFileConfigId)
  UNION
  (SELECT [ActionDetailDataChangeId], [ActionOverviewID], [Name] EntityName FROM [Logs].[ActionDetailDataChange] JOIN [dbo].[DmsImportSourceConfig] ON EntityType = 'DmsImportSourceConfig' AND EntityId = DmsImportSourceConfigId)
  UNION
  (SELECT [ActionDetailDataChangeId], [ActionOverviewID], [RootUrl] EntityName FROM [Logs].[ActionDetailDataChange] JOIN [dbo].[WebServer] ON EntityType = 'WebServer' AND EntityId = WebServerId)
GO
