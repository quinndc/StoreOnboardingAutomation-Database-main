USE [StoreOnboardingAutomation]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--DROP PROCEDURE [dbo].[StoreDecom_Users]
--GO



CREATE PROCEDURE [dbo].[StoreDecom_Users]
    @StoreID VARCHAR(5)
	

AS
BEGIN

SET NOCOUNT ON;


    DECLARE @sql NVARCHAR(MAX),
		@IP VARCHAR(50),
		@leadcrumb VARCHAR(50),
		@Count INT = 1,
		@Total INT = 0,
		@GalaxyUserGUID VARCHAR(100);


	SELECT 
	 @IP = ds.InternalServerIP,
	 @leadcrumb = ds.DatabaseName

	FROM Galaxy.Galaxy.dbo.Store s
	LEFT JOIN Galaxy.Galaxy.dbo.StoreDriveServerLink sdsl
		ON s.pkStoreID = sdsl.fkStoreID
	LEFT JOIN Galaxy.Galaxy.dbo.DriveServer ds
		ON sdsl.fkDriveServerID = ds.pkDriveServerID
	LEFT JOIN Galaxy.[DBAAdmin].[dbo].[_ServerList] sl
		ON ds.InternalServerIP = CAST(sl.ip_address AS VARCHAR(MAX))
	WHERE s.isDeleted = 0
		AND s.pkStoreID = @StoreID




	SET @sql = N'

	IF OBJECT_ID(''tempdb..##galaxyuserguid'') IS NOT NULL
	DROP TABLE ##galaxyuserguid

	SELECT 
		GalaxyUserGUID
	INTO ##galaxyuserguid
	FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].[User] u
	WHERE u.fkStoreID = ' + @StoreID +'
		AND u.Email not like ''%@drivecentric%''	
        AND u.pkUserID NOT IN (

SELECT DISTINCT u.pkUserID FROM (
    SELECT ROW_NUMBER()OVER (PARTITION BY fkuserid ORDER BY fkstoreid) AS row_num, * 
    FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].[StoreUser]) s
JOIN  [' + @IP + '].[' + @leadcrumb + '].[dbo].[User] u ON u.pkUserID = s.fkUserID
WHERE u.fkStoreID = ' + @StoreID +'
and row_num >1
	) 



	UPDATE gu 
	SET IsDeleted = 1, 
		IsActive = 0, 
		DateModified = CURRENT_TIMESTAMP
    --SELECT gu.*
	FROM [GALAXY].[Galaxy].dbo.GalaxyUser gu
	JOIN ##galaxyuserguid gg ON gg.galaxyuserguid = gu.guid
	WHERE gu.UserType <> 5 ;
'

EXEC sp_executesql @sql


END

GO


