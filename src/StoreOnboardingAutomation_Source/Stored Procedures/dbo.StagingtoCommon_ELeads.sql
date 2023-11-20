SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO


CREATE PROCEDURE [dbo].[StagingtoCommon_ELeads]   
	@GUID UNIQUEIDENTIFIER

AS


/*

ELeads Staging to Common

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
		@ParentStage INT = (SELECT pkImportParentStageID FROM [StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'StagingtoCommon_ELeads'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(300),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@ErrorMessage  NVARCHAR(4000), 
		@ErrorSeverity INT, 
		@ErrorState    INT,
		@CompletionCheck BIT;



------------------------------------
--  INSERT COMMONUSER
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonUser')

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
					[lUserID],
					CURRENT_TIMESTAMP,
					' + @ImportID + ',
					1,
					CONVERT(VARCHAR(32), [szFirstName]), 
					CONVERT(VARCHAR(32), [szLastName]),
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iUsers_' + @StoreID + '
				WHERE [lUserID] IS NOT NULL'

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
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonCustomer')

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
					,[BuyerType]
					,[PrimaryFirstName]
					,[PrimaryLastName]
					,[PrimaryDOB]
					,[Address1]
					,[Address2]
					,[City]
					,[State]
					,[Zip]
					,[CustomerType]
					,[CompanyName]
					,[PrimaryMiddleName]
					,[DateModified])
				SELECT
					c.[lPersonID],
					CASE 
						WHEN ISDATE(c.[dtEntry]) = 1 THEN CONVERT(DATETIME,c.[dtEntry]) 
						ELSE '''' 
					END AS DateCreated,
					' + @ImportID + ',
					1,
					CASE 
						WHEN c.szPrefix = ''Business'' THEN '''' 
						ELSE CONVERT(VARCHAR(32),c.[szFirstName]) 
					END AS PrimaryFirstName,
					CASE 
						WHEN c.szPrefix = ''Business'' THEN '''' 
						ELSE CONVERT(VARCHAR(32),c.[szLastName]) 
					END AS PrimaryLastName,
					CASE 
						WHEN ISDATE(c.[dtBirthday]) = 1 THEN CONVERT(DATETIME,c.[dtBirthday]) 
						ELSE NULL
					END AS PrimaryDOB,
					CONVERT(VARCHAR(128),c.szAddress1) AS Address1,
					ISNULL(CONVERT(VARCHAR(128), c.[szAddress2]), '''') AS Address2,
					CONVERT(VARCHAR(32),c.[szCity]) AS City,
					CONVERT(VARCHAR(2),c.[szAbbreviation]) AS [State],
					CONVERT(VARCHAR(10),c.[szZip]) AS Zip,
					CASE 
						WHEN c.szPrefix = ''Business'' THEN 10 
						ELSE 9 
					END AS CustomerType,
					CASE 
						WHEN c.szPrefix = ''Business'' THEN CONVERT(VARCHAR(128),c.szLastName) 
						ELSE '''' END AS CompanyName,

					ISNULL(CONVERT(VARCHAR(256), c.[szMiddleName]), '''') as PrimaryMiddleName,
					CURRENT_TIMESTAMP AS DateModified
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

-----------------------------------------
--  INSERT COMMONCUSTOMERCONTACT - EMAIL
-----------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonCustomerContact - Email')

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
				SELECT MAX(e.lPersonID),
					MAX(CONVERT(DATETIME, e.dtEntry)),
					' + @ImportID + ',
					1, -- personal
					2, -- email
					LOWER(CONVERT(VARCHAR(320), e.szAddress)),
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iEmails_' + @StoreID + ' e
				WHERE ' + @TableLocation + 'fnisvalidemail(e.szaddress) = 1
				group by e.lPersonID, e.szAddress '

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
--  INSERT COMMONCUSTOMERCONTACT - PHONES
------------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonCustomerContact - Phones')

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
				SELECT MAX(p.lPersonID),
					MAX(CONVERT(DATETIME, p.dtEntry)),
					' + @ImportID + ',
					CASE
						WHEN MAX(p.szPhoneType) = ''Home Phone'' THEN 1
						WHEN MAX(p.szPhoneType) = ''Work Phone'' THEN 3
						WHEN MAX(p.szPhoneType) = ''Cellular'' THEN 2
						ELSE 1
					END,
					1, -- phone
					CONVERT(VARCHAR(10), (p.szAreaCode + p.szNumber)),
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iPhones_' + @StoreID + ' p
				WHERE LEN(p.szAreaCode + p.szNumber) = 10
				GROUP BY p.lPersonID, p.szAreaCode + p.szNumber '

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
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonDeal')

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

				--CREATE UNIQUE USERSALES1 TABLE
				SELECT lDealID, MAX(lSalespersonID) lSalespersonID, szPosition, bPositionPrimary INTO #UserSales1
				FROM ' + @TableLocation + '_iDealUser_' + @StoreID + ' 
				WHERE bPositionPrimary = ''TRUE'' AND szPosition = ''Salesperson''
				GROUP BY lDealID, szPosition, bPositionPrimary

				--CREATE UNIQUE USERSALES2 TABLE
				SELECT lDealID, MAX(lSalespersonID) lSalespersonID, szPosition, bPositionPrimary INTO #UserSales2
				FROM ' + @TableLocation + '_iDealUser_' + @StoreID + ' 
				WHERE bPositionPrimary = ''FALSE'' AND szPosition = ''Salesperson''
				GROUP BY lDealID, szPosition, bPositionPrimary

				--CREATE UNIQUE USERBDC TABLE
				SELECT lDealID, MAX(lSalespersonID) lSalespersonID, szPosition, bPositionPrimary INTO #UserBDC
				FROM ' + @TableLocation + '_iDealUser_' + @StoreID + ' 
				WHERE bPositionPrimary = ''TRUE'' AND szPosition = ''BDC Agent''
				GROUP BY lDealID, szPosition, bPositionPrimary


				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonDeal](
					[CRMDealID]
					,[DealDateCreated]
					,[FKImportLogID]
					,[FKBuyerID]
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
					,[SoldNotDeliveredFlag]
					,[OrderedFlag]
					,[PendingFlag]
					)
				SELECT 
					d.[lDealID],
					CONVERT(DATETIME, d.[dtProspectIn]) AS DateCreated,
					' + @ImportID + ',
					d.[lPersonID],
					ISNULL(du1.[lSalesPersonID],'''') AS FKUserIDSales1,
					ISNULL(du2.[lSalesPersonID],'''') AS FKUserIDSales2,
					ISNULL(dub.[lSalesPersonID],'''') AS FKUserIDBDC,
					d.[lPersonID] AS fkCustomerID,
					d.[bDelivered] AS Delivered,
					CASE 
						WHEN d.[dtSold] = '''' THEN NULL
						ELSE d.[dtSold]
					END AS SoldDate,
					CASE
						WHEN d.szUpType = ''Showroom Up'' THEN 1
						WHEN d.szUpType = ''Internet Up'' THEN 6
						WHEN d.szUpType = ''Phone Up'' THEN 3
						WHEN d.szUpType = ''Campaign'' THEN 25
						ELSE 6 -- Undefined
					END AS SourceType,
					CASE 
						WHEN d.szSourceDetails IS NOT NULL THEN CONVERT(VARCHAR(100), d.[szUpSource]) + '' - '' + CONVERT(VARCHAR(100), d.szSourceDetails)
						ELSE CONVERT(VARCHAR(264), d.[szUpSource]) 
					END AS SourceDescription,
					CASE
						WHEN d.szStatus=''Inactive'' AND d.dtClosed IS NOT NULL THEN d.dtClosed
						ELSE NULL
					END,
					CURRENT_TIMESTAMP AS DateModified,
					CASE
						WHEN d.szStatus = ''Active'' AND d.szDealSubStatus IN (''On Order'', ''Pre Orders'') THEN 1
						ELSE 0
					END AS SoldNotDeliveredFlag,
					CASE
						WHEN d.szStatus = ''Active'' AND d.szDealSubStatus IN (''On Order'') THEN 1
						ELSE 0
					END AS OrderedFlag,
					CASE
						WHEN d.szStatus = ''Active'' AND d.szDealSubStatus IN (''Pre Orders'') THEN 1
						ELSE 0
					END AS PendingFlag
				FROM ' + @TableLocation + '_iDeals_' + @StoreID + ' d
				LEFT JOIN #UserSales1 du1
				ON du1.[lDealID] = d.[lDealID]
				LEFT JOIN #UserSales2 du2
				ON du2.[lDealID] = d.[lDealID]
				LEFT JOIN #UserBDC dub
				ON dub.[lDealID] = d.[lDealID]

				WHERE d.[lDealID] IS NOT NULL 
					AND ISDATE(d.[dtProspectIn]) = 1 '

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
	
--------------------------------------
----  TIE PROPOSAL DF DATE TO DEALS
--------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Tie Proposal DF Date to Deals')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Tie Proposal DF Date to Deals';

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

				UPDATE d
				SET d.ProposalDealFlag = it.dtDate
				FROM [StoreOnboardingAutomation].[dbo].[CommonDeal] d 
					INNER JOIN ' + @TableLocation + '_itaskitem_' + @StoreID + ' it on it.ldealid = d.CRMDealId
				WHERE it.szListItem=''Write Up'' AND it.dtDate <> ''''
					AND d.fkImportLogID =' + @ImportID + ''

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
--  TIE USERS TO CUSTOMER
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Tie Users to Customer')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Tie Users to Customer';

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

				UPDATE c
				SET c.Sales1UserID = ISNULL(d.fkUserIDSales1, 0),
					c.Sales2UserID = ISNULL(d.fkUserIDSales2, 0),
					c.BDCUserID = ISNULL(d.fkUserIDBDC, 0)
				FROM [StoreOnboardingAutomation].[dbo].[CommonCustomer] c 
					INNER JOIN [StoreOnboardingAutomation].[dbo].[CommonDeal] d ON c.[CRMCustomerID] = d.[FKCustomerID] AND c.[FKImportLogID] = d.[FKImportLogID]
				WHERE c.fkImportLogID =' + @ImportID + '
				AND d.fkImportLogID =' + @ImportID + '' 

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
	
----------------------------------------------------------
--  INSERT COMMONCUSTOMERVEHICLE - TIE VEHICLES TO DEALS
----------------------------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonCustomerVehicle - Tie Vehicles to Deals')

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

				--CREATE UNIQUE USERSALES1 TABLE
				SELECT lDealID, MAX(lSalespersonID) lSalespersonID, szPosition, bPositionPrimary INTO #UserSales1
				FROM ' + @TableLocation + '_iDealUser_' + @StoreID + ' 
				WHERE bPositionPrimary = ''TRUE'' AND szPosition = ''Salesperson''
				GROUP BY lDealID, szPosition, bPositionPrimary

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
					d.lPersonID,
					CONVERT(DATETIME, d.[dtProspectIn]),
					' + @ImportID + ',
					du.[lSalesPersonID] AS Sales1UserID,
					d.lDealID,
					IIF(v.bNewUsed = ''True'',2,1) AS NewUsedType,
					1 AS InterestType,
					ISNULL(CONVERT(int, CASE WHEN ISDATE(v.dtModelYear) = 1 THEN DATEPART(year, v.dtModelYear) else 0 END), 0) AS [Year],
					CONVERT(VARCHAR(64), REPLACE(ISNULL(v.szMake, ''''), ''"'', '''')) AS Make,
					CONVERT(VARCHAR(64), REPLACE(ISNULL(v.szModel, ''''), ''"'', '''')) AS [Model],
					CONVERT(VARCHAR(32), REPLACE(ISNULL(v.szTrim, ''''), ''"'', '''')) AS Trim,
					0,
					CONVERT(VARCHAR(17), v.szSoughtVIN) AS VIN,
					CONVERT(VARCHAR(64), v.szStockNumber),
					CURRENT_TIMESTAMP AS DateModified
				FROM ' + @TableLocation + '_iDeals_' + @StoreID + ' d	
					INNER JOIN ' + @TableLocation + '_iVehicles_' + @StoreID + ' v ON v.lDealID = d.lDealID
					LEFT JOIN #UserSales1 du ON du.[lDealID] = d.[lDealID]
				WHERE v.szMake <> '''' AND d.[lDealID] IS NOT NULL '

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
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonTask - Calls')

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

				SELECT * INTO #Calls FROM ' + @TableLocation + '_iActivities_' + @StoreID + '
				WHERE 	[szTaskType] in (''Phone Call'', ''Inbound Call'', ''Phone Up'')
					AND ISDATE(LEFT(dtCompleted, 19)) = 1;


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

				SELECT DISTINCT

					' + @ImportID + ',
					t.lCustomerID,
					ISNULL(u.[lUserID], 0),
					ISNULL(u.[lUserID], 0),
					ISNULL(u.[lUserID], 0),
					t.lDealID,
					LEFT(t.dtCompleted, 19),
					LEFT(t.dtCompleted, 19),
					LEFT(t.dtCompleted, 19),
					''Logged Call'',
					ISNULL(CONVERT(VARCHAR(8000), t.szComments1), ''''),
					LEFT(t.dtCompleted, 19),
					LEFT(t.dtCompleted, 19),
					CASE 
						WHEN [szTaskType] in (''Phone Up'') THEN 1
						WHEN t.szComments1 LIKE ''ava %'' THEN 0
						WHEN t.szComments1 LIKE ''%talked%'' THEN 1
						WHEN t.szComments1 LIKE ''%spoke%'' THEN 1
						WHEN t.szComments1 LIKE ''lm %'' THEN 2
						WHEN t.szComments1 LIKE ''% lm %'' THEN 2
						WHEN t.szComments1 LIKE ''% lm'' THEN 2
						WHEN t.szComments1 = ''lm'' THEN 2
						WHEN t.szComments1 LIKE ''left %'' THEN 2
						WHEN t.szComments1 = ''No Answer'' THEN 3
						WHEN t.szComments1 LIKE ''%Left Message%'' THEN 2
						WHEN t.szComments1 = ''Client was unreachable: vm'' THEN 2
						WHEN t.szComments1 = ''lm'' THEN 2
						WHEN t.szComments1 = ''lm'' THEN 2
						WHEN t.szComments1 LIKE ''reached%'' THEN 1
						WHEN t.szComments1 LIKE ''%reached%'' THEN 1
						ELSE 0
					END AS ResultType,
					CURRENT_TIMESTAMP

				FROM #Calls t 
				INNER JOIN ' + @TableLocation + '_iCustomers_' + @StoreID + ' c
					ON t.lCustomerID = c.lPersonID
				INNER JOIN ' + @TableLocation + '_iDeals_' + @StoreID + ' d
					ON t.lDealID = d.lDealID
				INNER JOIN ' + @TableLocation + '_iUsers_' + @StoreID + ' u
					ON u.lUserID = t.lCompletedByID'

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

--------------------------------------
--  INSERT COMMONCUSTOMERLOG - NOTES
--------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonCustomerLog - Notes')

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

				SELECT * INTO #Notes FROM ' + @TableLocation + '_iActivities_' + @StoreID + '
				WHERE 	[szTaskType] in (''Note'')
					AND ISDATE(LEFT(dtCompleted, 19)) = 1;

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonCustomerLog] (
						   [FKCustomerID]
						   ,[CustomerLogDateCreated]
						   ,[FKImportLogID]
						   ,[FKUserIDSales1]
						   ,[FKDealID]
						   ,[Notes]
						   ,[DateModified])
				SELECT 
					t.lCustomerID,
					LEFT(t.dtCompleted, 19),
					' + @ImportID + ',
					t.lCompletedByID,
					t.lDealID,
					CONVERT(VARCHAR(4000), t.szComments1),
					CURRENT_TIMESTAMP
				FROM #Notes t '

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
	
---------------------------------------------
--  INSERT COMMONDEALLOG - SHOWROOM VISITS
---------------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonDealLog - Showroom Visits')

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

				SELECT * INTO #Visits FROM ' + @TableLocation + '_iActivities_' + @StoreID + '
				WHERE 	[szTaskType] in (''Showroom Visit'')
					AND ISDATE(LEFT(dtCompleted, 19)) = 1;

				INSERT INTO [StoreOnboardingAutomation].[dbo].[CommonDealLog] (
						   [FKCustomerID]
						   ,[DealLogDateCreated]
						   ,[FKImportLogID]
						   ,[FKUserIDSales1]
						   ,[FKDealID]
						   ,[DealLogType]
						   ,[DateModified])
				SELECT 
					t.lCustomerID,
					LEFT(t.dtCompleted, 19),
					' + @ImportID + ',
					t.lCompletedByID,
					t.lDealID,
					dealLogType.Id,
					CURRENT_TIMESTAMP
				FROM #Visits t 
				LEFT JOIN ' + @TableLocation + 'fnNewSplit('','',''108,109,222'') dealLogType ON 1 = 1 '

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
-- INSERT COMMONCRUMB - EMAILS
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonCrumb - Emails')

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

				--Create Staging Table
				SELECT t.lCustomerID,
					t.lCompletedByID,
					t.lDealID,
					m.szSubject,
					m.szFrom,
					m.szTo,
					t.szTaskType,
					LEFT(CASE WHEN ISNULL(xEmailBody,'''') = '''' THEN m.szSubject ELSE ISNULL(xEmailBody,'''') END, 8000) AS EmailBody,
					LEFT(t.dtEntry, 19) as dtEntry
				INTO #StagedEmails
				FROM ' + @TableLocation + '_iMessages_' + @StoreID + ' m 
					INNER JOIN ' + @TableLocation + '_iActivities_' + @StoreID + ' t  on t.lTaskID=m.ltaskid
					LEFT JOIN ' + @TableLocation + '_iEmailBody_' + @StoreID + ' eb  on eb.lMessageId=m.lMessageId
				WHERE ISDATE(LEFT(t.dtEntry, 19)) = 1
					AND LEFT(CASE WHEN ISNULL(xEmailBody,'''') = '''' THEN m.szSubject ELSE ISNULL(xEmailBody,'''') END, 8000) IS NOT NULL

				CREATE CLUSTERED INDEX [ci] ON #StagedEmails (lCustomerID,lDealID)

				--Insert Email to CommonCrumb
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
					se.dtEntry,
					se.dtEntry,
					' + @ImportID + ',
					ISNULL(se.lCustomerID,0),
					ISNULL(se.lCompletedByID,0),
					ISNULL(se.lDealID,0),
					CASE
						WHEN se.szTaskType = ''Send Email'' THEN 10
						WHEN se.szTaskType = ''Read Email'' THEN 15
						WHEN se.szTaskType = ''Web Up'' THEN 15
						WHEN se.szTaskType = ''Auto Response'' THEN 19
						ELSE 19
					END AS CrumbType,
					LEFT(se.szSubject, 128),
					se.szFrom,
					se.szTo,
					se.EmailBody,
					se.dtEntry,
					se.EmailBody,
					CURRENT_TIMESTAMP	
				FROM #StagedEmails se '

		
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
-- INSERT COMMONCRUMB - TEXTS
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'StagingtoCommon_ELeads', 'Insert CommonCrumb - Texts')

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
					CASE
						WHEN it.Received = ''0001-01-01 00:00:00.000'' THEN ''''
						ELSE ISNULL(it.Received,'''')
					END,
					CURRENT_TIMESTAMP,
					' + @ImportID + ',
					ISNULL(it.lPersonID,0),
					ISNULL(u.lUserID,0),
					CASE
						WHEN it.TextSender = ''Customer'' THEN 33
						WHEN it.TextSender = ''Dealership'' THEN 2
						ELSE 0
					END AS CrumbType,
					CASE 
						WHEN it.TextSender = ''Customer'' THEN 0
						WHEN it.TextSender = ''Dealership'' THEN 1
						ELSE 0
					END AS IsSent,
					CASE 
						WHEN it.Success = ''True'' THEN 1
						WHEN it.Success = ''False'' THEN 0
						ELSE 0
					END as IsRead,
					--FROM
					CASE 
						WHEN it.TextSender = ''Customer'' THEN it.CustomerNumber
						WHEN it.TextSender = ''Dealership'' THEN ''' + @DealerPhone + '''
						ELSE '''' 
					END,
					--TO
					CASE 
						WHEN it.TextSender = ''Customer'' THEN ''' + @DealerPhone + '''
						WHEN it.TextSender = ''Dealership'' THEN it.CustomerNumber
						ELSE '''' 
					END,
					ISNULL(CONVERT(VARCHAR(8000), it.SMSContent),''''),
					CASE
						WHEN it.Received = ''0001-01-01 00:00:00.000'' THEN ''''
						ELSE ISNULL(it.Received,'''')
					END,
					ISNULL(it.SMSContent,''''),
					CURRENT_TIMESTAMP
				FROM ' + @TableLocation + '_iTexts_' + @StoreID + ' it 
					LEFT JOIN ' + @TableLocation + '_iUsers_' + @StoreID + ' u ON u.szfirstname + '' '' + u.szlastname = it.EmployeeName '


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
SET @ChildStageName = 'StagingtoCommon_ELeads Successfully Completed';

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
		PRINT ''StagingtoCommon_ELeads has completed successfully.  Please check ImportHistory and ImportError for details'''


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
  
