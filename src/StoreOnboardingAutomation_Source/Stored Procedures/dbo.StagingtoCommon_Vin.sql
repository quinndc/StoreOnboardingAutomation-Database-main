SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[StagingtoCommon_Vin]   
	@GUID UNIQUEIDENTIFIER

AS


/*

Vin Staging to Common (CMS001) 

*/


DECLARE @ImportID VARCHAR(MAX)
DECLARE @StoreID VARCHAR(5) 
DECLARE @DealerPhone VARCHAR(10)
DECLARE @ManualDeliver BIT
DECLARE @ImportType INT
DECLARE @IP VARCHAR(50)
DECLARE @leadcrumb VARCHAR(50)
DECLARE @ServerName VARCHAR(50)
DECLARE @DynamicStoreID VARCHAR(25)
DECLARE @TableLocation VARCHAR(255)


SET NOCOUNT ON

-------------------------------------------------------------------
-- REQUIRED INPUT - ImportLog GUID
-------------------------------------------------------------------
SELECT TOP 1
	@ImportID = pkImportID,
	@StoreID = fKStoreID,
	@DealerPhone = DealerPhone,
	@ManualDeliver = ManualDeliver,
	@ImportType = ImportType
FROM [StoreOnboardingAutomation].[dbo].[ImportLog] 
WHERE GUID = @GUID --enter GUID here
	AND (DateCompleted = '' OR DateCompleted IS NULL)

--SELECT @ImportID, @StoreID, @DealerPhone, @ManualDeliver, @ImportType

SELECT 
 @IP = ds.InternalServerIP,
 @leadcrumb = ds.DatabaseName,
-- s.pkStoreID, s.Name AS StoreName, ds.pkDriveServerId, ds.Name as StarName, ds.Description as StoreGroupDesc, ds.RootURL, ds.InternalServerIP
 @ServerName = sl.Server_name
-- , ds.DatabaseName
FROM Galaxy.Galaxy.dbo.Store s
LEFT JOIN Galaxy.Galaxy.dbo.StoreDriveServerLink sdsl
	ON s.pkStoreID = sdsl.fkStoreID
LEFT JOIN Galaxy.Galaxy.dbo.DriveServer ds
	ON sdsl.fkDriveServerID = ds.pkDriveServerID
LEFT JOIN Galaxy.[DBAAdmin].[dbo].[_ServerList] sl
	ON ds.InternalServerIP = CAST(sl.ip_address AS VARCHAR(MAX))
WHERE s.isDeleted = 0
    AND s.pkStoreID = @StoreID



--Setup for Initial Import
IF @ImportType = 1
	BEGIN
		SET @DynamicStoreID = '_'+ @StoreID
		SET @TableLocation = '[Staging].dbo.'
	END

--Setup for Gap Import
IF @ImportType = 2 
	BEGIN
		SET @DynamicStoreID = '_GAP_'+ @StoreID
		SET @TableLocation = '[' + @leadcrumb + '].dbo.'
	END
	
IF @ImportType NOT IN (1,2)
	BEGIN
		RAISERROR('This script does not support the ImportType value in the ImportLog record.  Please check the ImportLog record and make sure ImportType = 1 for initial imports or ImportType = 2 for gap imports.',16,1)
	END


-------------------------------------------------------------------
-- VARIABLES
-------------------------------------------------------------------

DECLARE @Rows INT,
		@ParentStage INT = (SELECT pkImportParentStageID FROM [StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'StagingtoCommon_VIN'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(300),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@ErrorMessage  NVARCHAR(4000), 
		@ErrorSeverity INT, 
		@ErrorState    INT,
		@CompletionCheck BIT;



------------------------------------
-- INSERT COMMONUSER
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonUser')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonUser';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonUser] (
					[CRMUserID]
					,[UserDateCreated]
					,[FKImportLogID]
					,[FKUserTypeID]
					,[FirstName]
					,[LastName]
					,[DateModified])
				SELECT 
					[UserID],
					CURRENT_TIMESTAMP,
					' + @ImportID + ',
					1, --Sales1
					CONVERT(VARCHAR(32), [FirstName]), 
					CONVERT(VARCHAR(32), [LastName]),
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iUsers_' + @StoreID + '
				WHERE [UserID] IS NOT NULL'

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  FLAG BDC USERS
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Flag BDC User')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Flag BDC User';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SELECT DISTINCT BDAgentUserID INTO #TempBDC FROM ' + @TableLocation + '_iCustomers_' + @StoreID + ' WHERE (BDAgentUserID <> '''' OR BDAgentUserID IS NOT NULL)

				UPDATE u
				SET u.BDCUser = 1
				FROM [StoreOnboardingAutomation].[dbo].[CommonUser] u 
					INNER JOIN #TempBDC t ON t.BDAgentUserID = u.CRMUserID
				WHERE u.FKImportLogID = ' + @ImportID

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  INSERT COMMONCUSTOMER
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonCustomer')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonCustomer';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonCustomer] (
					[CRMCustomerID]
					,[CustomerDateCreated]
					,[FKImportLogID]
					,[Sales1UserID]
					,[BDCUserID]
					,[BuyerType]
					,[PrimaryFirstName]
					,[PrimaryLastName]
					,[Email]
					,[PrimaryDOB]
					,[Address1]
					,[Address2]
					,[City]
					,[State]
					,[Zip]
					,[CustomerType]
					,[CompanyName]
					,[PrimaryMiddleName]
					,[DateModified]
					,[DoNotCall]
					,[DoNotEmail]
					,[DoNotMail])
				SELECT
					c.[GlobalCustomerID],
					CASE 
						WHEN ISDATE(c.[CreatedUTC]) = 1 THEN CONVERT(DATETIME,c.[CreatedUTC]) 
						ELSE '''' 
					END AS DateCreated,
					' + @ImportID + ',
					ISNULL(c.CurrentSalesRepUserID, '''') AS Sales1UserID,
					ISNULL(c.BDAgentUserID, '''') AS BDCUserID,
					1,
					CONVERT(VARCHAR(32),c.FirstName) AS PrimaryFirstName,
					CONVERT(VARCHAR(32),c.LastName) AS PrimaryLastName,
					CASE
						WHEN ' + @TableLocation + 'fnIsValidEmail(CONVERT(VARCHAR(128),c.email)) = 1 THEN CONVERT(VARCHAR(128), LOWER(c.email))
					ELSE ''''
					END AS Email,
					CASE 
						WHEN ISDATE(c.[Birthday]) = 1 THEN CONVERT(DATETIME,c.[Birthday]) 
						ELSE NULL
					END AS PrimaryDOB,
					CONVERT(VARCHAR(128),c.Address) AS Address1,
					'''' AS Address2,
					CONVERT(VARCHAR(32),c.[City]) AS City,
					CONVERT(VARCHAR(2),c.[State]) AS [State],
					CONVERT(VARCHAR(10),c.[PostalCode]) AS Zip,
					CASE 
						WHEN c.CompanyName <> '''' THEN 10 
						ELSE 9 
					END AS CustomerType,
					CONVERT(VARCHAR(128), c.CompanyName)  AS CompanyName,
					ISNULL(c.[MiddleName], '''') as PrimaryMiddleName,
					CURRENT_TIMESTAMP AS DateModified,
					CASE 
						WHEN c.DoNotCall = ''TRUE'' THEN 1
						ELSE 0
					END AS DoNotCall,
					CASE 
						WHEN c.DoNotEmail = ''TRUE'' THEN 1
						ELSE 0
					END AS DoNotEmail,
					CASE 
						WHEN c.DoNotMail = ''TRUE'' THEN 1
						ELSE 0
					END AS DoNotMail
				FROM ' + @TableLocation + '_iCustomers_' + @StoreID + ' c
				'

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

----------------------------------------
--  INSERT COMMONCUSTOMERCONTACT - EMAIL
----------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonCustomerContact - Email')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonCustomerContact - Email';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonCustomerContact] (
					[FKCustomerID]
					,[CustomerContactDateCreated]
					,[FKImportLogID]
					,[ContactLabelType]
					,[CommunicationType]
					,[Value]
					,[DateModified])
				SELECT 
					MAX(e.GlobalCustomerID),
					CASE 
						WHEN ISDATE(e.[CreatedUTC]) = 1 THEN CONVERT(DATETIME,e.[CreatedUTC]) 
						ELSE CURRENT_TIMESTAMP 
					END AS DateCreated,
					' + @ImportID + ',
					1, -- personal
					2, -- email
					LOWER(e.Email),
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iCustomers_' + @StoreID + ' e
				WHERE ' + @TableLocation + 'fnisvalidemail(e.Email) = 1
				GROUP BY e.GlobalCustomerID, e.Email, e.CreatedUTC'

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------------
--  Insert CommonCustomerContact - Phones
------------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonCustomerContact - Phones')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonCustomerContact - Phones';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonCustomerContact] (
					[FKCustomerID]
					,[CustomerContactDateCreated]
					,[FKImportLogID]
					,[ContactLabelType]
					,[CommunicationType]
					,[Value]
					,[DateModified])
				SELECT 
					MAX(p.GlobalCustomerID),
					CASE 
						WHEN ISDATE(p.[CreatedUTC]) = 1 THEN CONVERT(DATETIME,p.[CreatedUTC]) 
						ELSE CURRENT_TIMESTAMP 
					END AS DateCreated,
					' + @ImportID + ',
					1, --homephone
					1, -- phone
					p.EvePhone,
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iCustomers_' + @StoreID + ' p
				WHERE LEN(p.EvePhone) = 10
				GROUP BY p.GlobalCustomerID, p.EvePhone, p.CreatedUTC 

				UNION

				SELECT 
					MAX(p.GlobalCustomerID),
					CASE 
						WHEN ISDATE(p.[CreatedUTC]) = 1 THEN CONVERT(DATETIME,p.[CreatedUTC]) 
						ELSE CURRENT_TIMESTAMP 
					END AS DateCreated,
					' + @ImportID + ',
					2, -- cellphone
					1, -- phone
					p.CellPhone,
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iCustomers_' + @StoreID + ' p
				WHERE LEN(p.CellPhone) = 10
				GROUP BY p.GlobalCustomerID, p.CellPhone, p.CreatedUTC 

				UNION
				
				SELECT 
					MAX(p.GlobalCustomerID),
					CASE 
						WHEN ISDATE(p.[CreatedUTC]) = 1 THEN CONVERT(DATETIME,p.[CreatedUTC]) 
						ELSE CURRENT_TIMESTAMP 
					END AS DateCreated,
					' + @ImportID + ',
					3, -- workphone
					1, -- phone
					p.DayPhone,
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iCustomers_' + @StoreID + ' p
				WHERE LEN(p.DayPhone) = 10
				GROUP BY p.GlobalCustomerID, p.DayPhone, p.CreatedUTC '

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  INSERT COMMONDEAL
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonDeal')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonDeal';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonDeal](
					[CRMDealID]
					,[DealDateCreated]
					,[FKImportLogID]
					,[FKBuyerID]
					,[FKCoBuyerID]
					,[FKUserIDSales1]
					,[FKUserIDSales2]
					,[FKuserIDBDC]
					,[FKCustomerID]
					,[Delivered]
					,[SoldDate]
					,[SourceType]
					,[SourceDescription]
					,[InactiveDealFlag]
					,[DateModified]
					,[SoldFlag]
					,[SoldNotDeliveredFlag]
					,[OrderedFlag]
					,[PendingFlag]
					,[DeadFlag]
					,[ServiceFlag]
					)
				SELECT 
					d.[AutoLeadID],
					CONVERT(DATETIME, d.[Created]) AS DateCreated,
					' + @ImportID + ',
					d.[GlobalCustomerID],
					d.[CoBuyerGlobalCustomerID],
					CASE
						WHEN (d.CreatedByUserID IS NOT NULL OR d.CreatedByUserID <> '''') THEN d.CreatedByUserID
						WHEN (d.CreatedByUserID IS NULL OR d.CreatedByUserID = '''' AND c.CurrentSalesRepUserID <> '''') THEN c.CurrentSalesRepUserID
						ELSE 0
					END AS FKUserIDSales1,
					0 AS FKUserIDSales2,
					0 AS FKUserIDBDC,
					d.[GlobalCustomerID] AS fkCustomerID,
					0 AS Delivered,
					'''' AS SoldDate,
					CASE
						WHEN LeadType = ''Service'' THEN 5
						WHEN LeadType = ''Showroom Floor'' THEN 1
						WHEN LeadType = ''Walk-in'' THEN 1
						WHEN LeadType = ''Third Party'' THEN 6
						WHEN LeadType = ''Internet'' THEN 6
						WHEN LeadType = ''WebsiteChat'' THEN 6
						WHEN LeadType = ''Phone'' THEN 3
						ELSE 6  -- Undefined
					END AS SourceType,
					CONVERT(VARCHAR(264), RTRIM(LTRIM(d.LeadSourceName))) AS SourceDescription,
					NULL AS InactiveDealFlag,
					CURRENT_TIMESTAMP AS DateModified,
					CASE
						WHEN d.leadstatusname = ''On Order'' THEN 1
						ELSE 0
					END AS SoldFlag,
					0 AS SoldNotDeliveredFlag,
					CASE
						WHEN d.leadstatusname = ''On Order'' THEN 1
						ELSE 0
					END AS OrderedFlag,
					0 AS PendingFlag,
					CASE 
						WHEN d.leadstatus IN (''Bad'',''Lost'') THEN 1
						ELSE 0
					END AS DeadFlag,
					CASE
						WHEN LeadType = ''Service'' THEN 1
						ELSE 0
					END AS ServiceFlag
				FROM ' + @TableLocation + '_iDeals_' + @StoreID + ' d
				LEFT JOIN ' + @TableLocation + '_iCustomers_' + @StoreID + ' c ON c.GlobalCustomerID = d.GlobalCustomerID
				WHERE d.[AutoLeadID] IS NOT NULL 
					AND ISDATE(d.[Created]) = 1 '

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

--------------------------------------------------------
--  INSERT COMMONCUSTOMERVEHICLE - TIE VEHICLES TO DEALS
--------------------------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonCustomerVehicle - Tie Vehicles to Deals')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonCustomerVehicle - Tie Vehicles to Deals';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				INSERT INTO [StoreOnboardingAutomation].dbo.[CommonCustomerVehicle](
					[FKCustomerID]
					,[VehicleDateCreated]
					,[FKImportLogID]
					,[Sales1UserID]
					,[FKDealID]
					,[NewUsedType]
					,[InterestType]
					,[Year]
					,[Make]
					,[Model]
					,[Trim]
					,[OdometerStatus]
					,[VIN]
					,[StockNumber]
					,[DateModified])
				SELECT 
					d.GlobalCustomerID,
					CONVERT(DATETIME, d.[Created]),
					' + @ImportID + ',
					d.[CreatedByUserID] AS Sales1UserID,
					d.AutoLeadID,
					CASE
						WHEN d.VOI_InventoryType = ''N'' THEN 1 
						WHEN d.VOI_InventoryType = ''U'' THEN 2
						ELSE 0
					END AS NewUsedType,
					1 AS InterestType,
					ISNULL(CONVERT(INT, 
						CASE 
							WHEN d.VOI_Year < 1900 THEN 0 
							ELSE d.VOI_Year 
						END), 0) AS [Year],
					CONVERT(VARCHAR(64), REPLACE(ISNULL(d.VOI_Make, ''''), ''"'', '''')) AS Make,
					CONVERT(VARCHAR(64), REPLACE(ISNULL(d.VOI_Model, ''''), ''"'', '''')) AS [Model],
					'''' AS Trim,
					0,
					CONVERT(VARCHAR(17), d.VOI_VIN) AS VIN,
					CONVERT(VARCHAR(64), d.VOI_StockNumber) AS StockNumber,
					CURRENT_TIMESTAMP AS DateModified
				FROM ' + @TableLocation + '_iDeals_' + @StoreID + ' d	
				WHERE d.VOI_Make <> '''' AND d.[AutoLeadID] IS NOT NULL '

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

----------------------------------------------------
--  INSERT COMMONCUSTOMERVEHICLE - INSERT TRADES
----------------------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonCustomerVehicle - Insert Trades')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonCustomerVehicle - Insert Trades';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				INSERT INTO [StoreOnboardingAutomation].dbo.[CommonCustomerVehicle](
					[FKCustomerID]
					,[VehicleDateCreated]
					,[FKImportLogID]
					,[Sales1UserID]
					,[FKDealID]
					,[NewUsedType]
					,[InterestType]
					,[Year]
					,[Make]
					,[Model]
					,[Trim]
					,[OdometerStatus]
					,[VIN]
					,[StockNumber]
					,[DateModified])
				SELECT 
					d.GlobalCustomerID,
					CONVERT(DATETIME, d.[Created]),
					' + @ImportID + ',
					d.[CreatedByUserID] AS Sales1UserID,
					d.AutoLeadID,
					2 AS NewUsedType,
					4 AS InterestType, --trade
					ISNULL(CONVERT(INT, 
						CASE 
							WHEN t.[Year] < 1900 THEN 0 
							ELSE t.[Year] 
						END), 0) AS [Year],
					CONVERT(VARCHAR(64), REPLACE(ISNULL(t.[Make], ''''), ''"'', '''')) AS Make,
					CONVERT(VARCHAR(64), REPLACE(ISNULL(t.[Model], ''''), ''"'', '''')) AS [Model],
					'''' AS Trim,
					ISNULL(t.Mileage, 0),
					CONVERT(VARCHAR(17), t.VIN) AS VIN,
					'''' AS StockNumber,
					CURRENT_TIMESTAMP AS DateModified
				FROM ' + @TableLocation + '_iTrades_' + @StoreID + ' t
				INNER JOIN ' + @TableLocation + '_iDeals_' + @StoreID + ' d  ON t.AutoLeadID = d.AutoLeadID '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  INSERT COMMONTASK - CALLS
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonTask - Calls')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonTask - Calls';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SELECT * INTO #Calls FROM ' + @TableLocation + '_iNotes_' + @StoreID + '
				WHERE LeadMessageTypeName in (''Outbound phone call'', ''Incoming phone call'')
					AND ISDATE(Created) = 1;

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonTask](
					[FKImportLogID]
					,[FKCustomerID]
					,[FKUserID]
					,[FKCreatedByUserID]
					,[FKCompletedByUserID]
					,[FKDealID]
					,[TaskDateCreated]
					,[TaskDateModified]
					,[TaskDateCompleted]
					,[Subject]
					,[Resolution]
					,[DateStart]
					,[DateDue]
					,[ResultType]
					,[DateModified])
				SELECT 
					' + @ImportID + ',
					c.GlobalCustomerID,
					ISNULL(t.CreatedByUserID, 0),
					ISNULL(t.CreatedByUserID, 0),
					ISNULL(t.CreatedByUserID, 0),
					t.AutoLeadID,
					CONVERT(DATETIME, t.[Created]),
					CONVERT(DATETIME, t.[Created]),
					CONVERT(DATETIME, t.[Created]),
					''Logged Call'',
					ISNULL(t.MessageContent, ''''),
					CONVERT(DATETIME, t.[Created]),
					CONVERT(DATETIME, t.[Created]),
					CASE 
						WHEN t.MessageField2 = ''M'' then 2
						WHEN t.MessageField2 = ''Y'' then 1
						WHEN t.MessageField2 = ''N'' then 3
						WHEN t.MessageContent like ''%spoke to customer%'' then 1
						WHEN t.MessageContent like ''%left a message%'' then 2
						WHEN t.MessageContent like ''%left message%'' then 2
						WHEN t.MessageContent like ''%no answer%'' then 3
						WHEN t.MessageContent like ''%bad number%'' then 3
						ELSE 0
					END AS ResultType,
					CURRENT_TIMESTAMP
				FROM #Calls t 
				INNER JOIN ' + @TableLocation + '_iDeals_' + @StoreID + ' d
					ON t.AutoLeadID = d.AutoLeadID
				INNER JOIN ' + @TableLocation + '_iCustomers_' + @StoreID + ' c
					ON c.GlobalCustomerID = d.GlobalCustomerID '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  DELETE DUPLICATE TASK
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Delete Duplicate Task')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Delete Duplicate Task';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'
				DELETE t2
				FROM [StoreOnboardingAutomation].[dbo].[CommonTask] t1 
					INNER JOIN [StoreOnboardingAutomation].[dbo].[CommonTask] t2 ON t2.fkCustomerID=t1.fkCustomerID 
						AND t2.pkCommonTaskID > t1.pkCommonTaskID
						AND t2.Resolution = t1.Resolution
						AND len(t1.Resolution) > 3
						AND t2.TaskDateCreated = t1.TaskDateCreated
						AND t2.fkImportLogID = t1.fkImportLogID 
						AND t1.FKImportLogID = ' + @ImportID


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  INSERT COMMONCUSTOMERLOG - NOTES
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonCustomerLog - Notes')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonCustomerLog - Notes';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SELECT * INTO #Notes FROM ' + @TableLocation + '_iNotes_' + @StoreID + '
				WHERE LeadMessageTypeName in (''Note'')

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonCustomerLog] (
					[FKCustomerID]
					,[CustomerLogDateCreated]
					,[FKImportLogID]
					,[FKUserIDSales1]
					,[FKDealID]
					,[Notes]
					,[DateModified])
				SELECT 
					c.GlobalCustomerID,
					CONVERT(DATETIME, t.[Created]),
					' + @ImportID + ',
					ISNULL(t.CreatedByUserID, 0),
					t.AutoLeadID,
					CONVERT(VARCHAR(4000), t.MessageContent),
					CURRENT_TIMESTAMP
				FROM #Notes t 
				INNER JOIN ' + @TableLocation + '_iDeals_' + @StoreID + ' d
					ON t.AutoLeadID = d.AutoLeadID
				INNER JOIN ' + @TableLocation + '_iCustomers_' + @StoreID + ' c
					ON c.GlobalCustomerID = d.GlobalCustomerID '

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
-- NUKE THE DUPE NOTES
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Nuke The Dupe Notes')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Nuke The Dupe Notes';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				DELETE cl2
				FROM [StoreOnboardingAutomation].[dbo].[CommonCustomerLog] cl1 (nolock)
					INNER JOIN [StoreOnboardingAutomation].[dbo].[CommonCustomerLog] cl2 (nolock) ON 
						cl2.fkCustomerID=cl1.fkCustomerID 
						AND cl2.pkCommonCustomerLogID > cl1.pkCommonCustomerLogID
						AND cl2.Notes = cl1.Notes
						AND len(cl1.Notes) > 3
						AND cl2.CustomerLogDateCreated = cl1.CustomerLogDateCreated 
						AND cl1.fkImportLogID = cl2.fkImportLogID
						AND cl1.FKImportLogID = ' + @ImportID

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

-----------------------------------------------
--  INSERT COMMONDEALLOG - SHOWROOM VISITS
-----------------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonDealLog - Showroom Visits')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonDealLog - Showroom Visits';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SELECT * INTO #Visits FROM ' + @TableLocation + '_iNotes_' + @StoreID + '
				WHERE LeadMessageTypeName = (''Showroom Visit'')
					AND ISDATE(LEFT(Created, 19)) = 1;

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonDealLog] (
						   [FKCustomerID]
						   ,[DealLogDateCreated]
						   ,[FKImportLogID]
						   ,[FKUserIDSales1]
						   ,[FKDealID]
						   ,[DealLogType]
						   ,[DateModified])
				SELECT 
					c.GlobalCustomerID,
					LEFT(t.Created, 19),
					' + @ImportID + ',
					t.CreatedByUserID,
					t.AutoLeadID,
					dealLogType.Id,
					CURRENT_TIMESTAMP
				FROM #Visits t 
				INNER JOIN ' + @TableLocation + '_iDeals_' + @StoreID + ' d
					ON t.AutoLeadID = d.AutoLeadID
				INNER JOIN ' + @TableLocation + '_iCustomers_' + @StoreID + ' c
					ON c.GlobalCustomerID = d.GlobalCustomerID 
				LEFT JOIN ' + @TableLocation + 'fnNewSplit('','',''108,109,222'') dealLogType on 1=1 '

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  INSERT COMMONCRUMB - EMAILS
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonCrumb - Emails')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonCrumb - Emails';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SELECT * INTO #Emails FROM ' + @TableLocation + '_iNotes_' + @StoreID + '
				WHERE LeadMessageTypeName IN (''Email auto responder'', ''Sales rep emailed prospect'',''Email reply FROM customer'',''Email delivery failure'')
						AND ISDATE(MessageTime) = 1
						AND (CreatedByUserID <> '''' OR CreatedByUserID IS NOT NULL);
							   		

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonCrumb](
					[CrumbDateCreated]
					,[CrumbDateModified]
					,[FKImportLogID]
					,[FKCustomerID]
					,[FKUserID]
					,[FKDealID]
					,[CrumbType]
					,[Subject]
					,[From]
					,[To]
					,[StrippedMessage]
					,[DateRead]
					,[UnicodeStrippedMessage]
					,[DateModified])
				SELECT 
					CONVERT(DATETIME, e.MessageTime),
					CONVERT(DATETIME, e.MessageTime),
					' + @ImportID + ',
					ISNULL(c.GlobalCustomerID,0),
					ISNULL(e.CreatedByUserID,0),
					ISNULL(e.AutoLeadID,0),
					CASE
						WHEN LeadMessageTypeName = ''Email auto responder'' THEN 19
						WHEN LeadMessageTypeName = ''Sales rep emailed prospect'' THEN 10
						WHEN LeadMessageTypeName =  ''Email reply FROM customer'' THEN 15
						WHEN LeadMessageTypeName = ''Email delivery failure'' THEN 25
					END AS CrumbType,
					LEFT(e.MessageField1,128),
					e.messageField3,
					e.messageField2,
					LEFT(ISNULL(e.MessageContent, ''''), 8000),
					CONVERT(DATETIME, e.LastUpdated),
					e.MessageContent,
					CURRENT_TIMESTAMP	
				FROM #Emails e 
				INNER JOIN ' + @TableLocation + '_iDeals_' + @StoreID + ' d
					ON e.AutoLeadID = d.AutoLeadID
				INNER JOIN ' + @TableLocation + '_iCustomers_' + @StoreID + ' c
					ON c.GlobalCustomerID = d.GlobalCustomerID '

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  NUKE THE DUPE EMAILS
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Nuke The Dupe Emails')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Nuke The Dupe Emails';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				DELETE cr2
				FROM [StoreOnboardingAutomation].[dbo].[CommonCrumb] cr2
				WHERE EXISTS (SELECT 1 FROM [StoreOnboardingAutomation].[dbo].[CommonCrumb] cr1
						WHERE cr1.fkCustomerID=cr2.fkCustomerID 
						AND cr1.pkCommonCrumbID > cr2.pkCommonCrumbID
						AND cr1.StrippedMessage = cr2.StrippedMessage
						AND len(cr1.StrippedMessage) > 3
						AND cr1.CrumbDateCreated = cr2.CrumbDateCreated
						AND cr1.CrumbType = cr2.CrumbType
						AND cr1.fkImportLogID = cr2.fkImportLogID
						AND cr2.FKImportLogID = ' + @ImportID + ')'

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  INSERT COMMONCRUMB - TEXTS
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_VIN', 'Insert CommonCrumb - Texts')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CommonCrumb - Texts';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--ImportLog History
			INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonCrumb](
					[CrumbDateCreated]
					,[CrumbDateModified]
					,[FKImportLogID]
					,[FKCustomerID]
					,[FKUserID]
					,[CrumbType]
					,[IsSent]
					,[IsRead]
					,[From]
					,[To]
					,[StrippedMessage]
					,[DateRead]
					,[UnicodeStrippedMessage]
					,[DateModified])
				SELECT 
					CONVERT(DATETIME, it.CreatedUTC),
					CURRENT_TIMESTAMP,
					' + @ImportID + ',
					ISNULL(it.GlobalCustomerID,0),
					ISNULL(it.UserID,0),
					CASE
						WHEN it.direction = ''2'' THEN 33
						WHEN it.direction = ''1'' OR it.direction = ''3'' OR it.direction = ''5'' THEN 2
						ELSE 0
					END AS CrumbType,
					CASE 
						WHEN it.direction = ''1'' OR it.direction = ''3'' OR it.direction = ''5'' THEN 0
						WHEN it.direction = ''2'' THEN 1
						ELSE 1
					END AS IsSent,
					0 AS IsRead,
					--FROM
					it.SenderPhone,
					--TO
					it.ReceiverPhone,
					LEFT(ISNULL(it.[Message],''''),8000),
					CONVERT(DATETIME, it.CreatedUTC),
					ISNULL(it.Message,''''),
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iTexts_' + @StoreID + ' it '

				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
					([fkImportId]
					,[fkImportHistoryId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[DateCreated]
					,[ErrorNumber]
					,[ErrorSeverity]
					,[ErrorState]
					,[ErrorProcedure]
					,[ErrorLine]
					,[ErrorMessage])
				SELECT   
					@ImportID
					,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				SELECT 
					@ErrorMessage = ERROR_MESSAGE(), 
					@ErrorSeverity = ERROR_SEVERITY(), 
					@ErrorState = ERROR_STATE();

				-- return the error inside the CATCH block
				RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

				RETURN;
			END CATCH;
		END;

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  END OF SCRIPT SUCCESS MESSAGE
------------------------------------
SET @ChildStageName = 'StagingtoCommon_Vin Successfully Completed';

	--Pull Child Stage ID
	SET @ChildStage = (SELECT pkImportChildStageID FROM [StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);


	--ImportLog History
	INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportHistory]
			([fkImportId]
			,[fkImportParentStageId]
			,[fkImportChildStageID]
			,[StageStart]
			,[StageEnd]
			,[RowCount])
	SELECT	@ImportID
			,@ParentStage
			,@ChildStage
			,CURRENT_TIMESTAMP
			,NULL
			,NULL;

BEGIN
	BEGIN TRY
		BEGIN TRANSACTION;

		SET @sqlcmd = N'
		PRINT ''StagingtoCommon_Vin has completed successfully.  Please check ImportHistory and ImportError for details'''


		EXEC sp_executesql @sqlcmd
		SET @Rows = @@ROWCOUNT;
COMMIT;
		
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0  
		ROLLBACK TRANSACTION;  

		INSERT INTO [StoreOnboardingAutomation].[dbo].[ImportErrorLog]
			([fkImportId]
			,[fkImportHistoryId]
			,[fkImportParentStageId]
			,[fkImportChildStageID]
			,[DateCreated]
			,[ErrorNumber]
			,[ErrorSeverity]
			,[ErrorState]
			,[ErrorProcedure]
			,[ErrorLine]
			,[ErrorMessage])
		SELECT   
			@ImportID
			,(SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
			,@ParentStage
			,@ChildStage
			,GETDATE()
			,ERROR_NUMBER() 
			,ERROR_SEVERITY()
			,ERROR_STATE() 
			,ERROR_PROCEDURE() 
			,ERROR_LINE()
			,ERROR_MESSAGE();

		SELECT 
			@ErrorMessage = ERROR_MESSAGE(), 
			@ErrorSeverity = ERROR_SEVERITY(), 
			@ErrorState = ERROR_STATE();

		-- return the error inside the CATCH block
		RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN;
	END CATCH;
END;

UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
SET [RowCount] = @Rows,
	[StageEnd] = CURRENT_TIMESTAMP
WHERE [RowCount] IS NULL
	AND StageEnd IS NULL
	AND fkImportId = @ImportID
	AND fkImportParentStageId = @ParentStage
	AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
