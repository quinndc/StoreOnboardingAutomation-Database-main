SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

--DROP VIEW dbo.ActionView

CREATE   VIEW [Logs].[ActionView] AS
SELECT ActionOverview.ActionOverviewId,
	Action,
	Store.Name Target,
	StoreGroup.Name StoreGroup,
	ActionOverview.CreatedOn,
	DisplayName CreatedBy      
FROM Logs.ActionOverview
LEFT JOIN Store ON Store.StoreId = ActionOverview.StoreId
LEFT JOIN StoreGroup ON StoreGroup.StoreGroupId = ActionOverview.StoreGroupId
LEFT JOIN Security.[User] ON ActionOverview.CreatedBy = [User].UserId
GO
