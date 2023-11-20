SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[CopyCommon]   
	@GUID UNIQUEIDENTIFIER

AS

/* 

NEW ONBOARDING PROCESS

3.  Copy Common Tables From CMS to PROD -  Run on Leadcrumb server

SELECT pkImportID, fkStoreID, DealerPhone, ManualDeliver, GUID FROM [PRDENCMS001].[StoreOnboardingAutomation].ImportLog ORDER BY DateCreated DESC


*/

DECLARE @ImportID VARCHAR(MAX)
DECLARE @Debug INT = 0

DECLARE @StoreID VARCHAR(5) 
DECLARE @DealerPhone VARCHAR(10)
DECLARE @ManualDeliver BIT
DECLARE @IP VARCHAR(50)
DECLARE @leadcrumb VARCHAR(50)

SET NOCOUNT ON

SELECT 
@ImportID = pkImportID,
@StoreID = FKStoreID,
@DealerPhone = DealerPhone,
@ManualDeliver = ManualDeliver
FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportLog] 
WHERE GUID = @GUID  --enter GUID here
	AND (DateCompleted = '' OR DateCompleted IS NULL)

--SELECT @ImportID, @StoreID, @DealerPhone, @ManualDeliver


SELECT 
 @IP = ds.InternalServerIP,
 @leadcrumb = ds.DatabaseName
-- s.pkStoreID, s.Name AS StoreName, ds.pkDriveServerId, ds.Name as StarName, ds.Description as StoreGroupDesc, ds.RootURL, ds.InternalServerIP
--, sl.Server_name as ServerName
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


IF OBJECT_ID('tempdb..#ImportLog') IS NOT NULL
    DROP TABLE #ImportLog
IF OBJECT_ID('tempdb..#CrmImportSourceConfig') IS NOT NULL
    DROP TABLE #CrmImportSourceConfig
SELECT * INTO #ImportLog FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportLog] 
SELECT * INTO #CrmImportSourceConfig FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CrmImportSourceConfig] 


-------------------------------------------------------------------
-- VARIABLES
-------------------------------------------------------------------

DECLARE @Rows INT,
		@ParentStage INT = (SELECT pkImportParentStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'CopyCommon'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(100),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@LeadcrumbTable VARCHAR(100),
		@LeadcrumbTableStartCount INT,
		@CompletionCheck BIT;


------------------------------------
--  COMMONCRUMB
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonCrumb')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonCrumb';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCrumb] ON;

				INSERT INTO [_Common].[dbo].[CommonCrumb]
					([PKCommonCrumbId]
					,[CrumbDateCreated]
					,[CrumbDateModified]
					,[FKImportLogID]
					,[FKCustomerID]
					,[FKUserID]
					,[FKDealID]
					,[CrumbType]
					,[IsSent]
					,[IsRead]
					,[Subject]
					,[From]
					,[To]
					,[StrippedMessage]
					,[DateRead]
					,[UnicodeStrippedMessage]
					,[DateModified])

				SELECT 
					[PKCommonCrumbId]
					,[CrumbDateCreated]
					,[CrumbDateModified]
					,[FKImportLogID]
					,[FKCustomerID]
					,[FKUserID]
					,[FKDealID]
					,[CrumbType]
					,[IsSent]
					,[IsRead]
					,[Subject]
					,[From]
					,[To]
					,[StrippedMessage]
					,[DateRead]
					,[UnicodeStrippedMessage]
					,[DateModified]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonCrumb] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCrumb] OFF;'			
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  COMMONCUSTOMER
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonCustomer')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonCustomer';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCustomer] ON;

				INSERT INTO [_Common].[dbo].[CommonCustomer]
					([PKCommonCustomerId]
					,[CRMCustomerID]
					,[CustomerDateCreated]
					,[FKImportLogID]
					,[Sales1UserID]
					,[Sales2UserID]
					,[BDCUserID]
					,[BuyerType]
					,[PrimaryFirstName]
					,[PrimaryLastName]
					,[Email]
					,[CellPhone]
					,[PrimaryDOB]
					,[Address1]
					,[Address2]
					,[City]
					,[State]
					,[Zip]
					,[CustomerType]
					,[CompanyName]
					,[HomePhone]
					,[WorkPhone]
					,[PrimaryMiddleName]
					,[CoFirstName]
					,[CoLastName]
					,[CoDOB]
					,[CoEmail]
					,[DateModified]
					,[DoNotCall]
					,[DoNotEmail]
					,[DoNotMail])


				SELECT 
					[PKCommonCustomerId] 
					,[CRMCustomerID] 
					,[CustomerDateCreated] 
					,[FKImportLogID] 
					,[Sales1UserID] 
					,[Sales2UserID] 
					,[BDCUserID] 
					,[BuyerType] 
					,[PrimaryFirstName] 
					,[PrimaryLastName] 
					,[Email] 
					,[CellPhone] 
					,[PrimaryDOB] 
					,[Address1] 
					,[Address2] 
					,[City] 
					,[State] 
					,[Zip] 
					,[CustomerType] 
					,[CompanyName]
					,[HomePhone] 
					,[WorkPhone] 
					,[PrimaryMiddleName] 
					,[CoFirstName] 
					,[CoLastName] 
					,[CoDOB]
					,[CoEmail] 
					,[DateModified] 
					,[DoNotCall] 
					,[DoNotEmail] 
					,[DoNotMail]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonCustomer] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCustomer] OFF;'
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
-- COMMONCUSTOMERCONTACT
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonCustomerContact')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonCustomerContact';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCustomerContact] ON;

				INSERT INTO [_Common].[dbo].[CommonCustomerContact]
					([PKCommonCustomerContactId]
					,[FKCustomerID]
					,[CustomerContactDateCreated]
					,[FKImportLogID]
					,[ContactLabelType]
					,[CommunicationType]
					,[Value]
					,[DateModified])

				SELECT 
					[PKCommonCustomerContactId] 
					,[FKCustomerID] 
					,[CustomerContactDateCreated]
					,[FKImportLogID] 
					,[ContactLabelType] 
					,[CommunicationType] 
					,[Value] 
					,[DateModified]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonCustomerContact] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCustomerContact] OFF;'
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
-- COMMONCUSTOMERLOG
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonCustomerLog')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonCustomerLog';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCustomerLog] ON;

				INSERT INTO [_Common].[dbo].[CommonCustomerLog]
					([PKCommonCustomerLogId]
					,[FKCustomerID]
					,[CustomerLogDateCreated]
					,[FKImportLogID]
					,[FKUserIDSales1]
					,[FKDealID]
					,[Notes]
					,[DateModified])

				SELECT 
					[PKCommonCustomerLogId] 
					,[FKCustomerID] 
					,[CustomerLogDateCreated] 
					,[FKImportLogID] 
					,[FKUserIDSales1] 
					,[FKDealID] 
					,[Notes] 
					,[DateModified]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonCustomerLog] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCustomerLog] OFF'
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
-- COMMONCUSTOMERVEHICLE
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonCustomerVehicle')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonCustomerVehicle';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCustomerVehicle] ON;

				INSERT INTO [_Common].[dbo].[CommonCustomerVehicle]
					([PKCommonCustomerVehicleId]
					,[FKCustomerID]
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
					,[InteriorColor]
					,[ExteriorColor]
					,[VIN]
					,[StockNumber]
					,[DateModified])
				
				SELECT 
					[PKCommonCustomerVehicleId] 
					,[FKCustomerID] 
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
					,[InteriorColor] 
					,[ExteriorColor] 
					,[VIN] 
					,[StockNumber] 
					,[DateModified]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonCustomerVehicle] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonCustomerVehicle] OFF;'
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  COMMONDEAL
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonDeal')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonDeal';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonDeal] ON;

				INSERT INTO [_Common].[dbo].[CommonDeal]
					([PKCommonDealId]
					,[CRMDealID]
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
					,[ProposalDealFlag]
					,[InactiveDealFlag]
					,[DateModified]
					,[SoldNotDeliveredFlag]
					,[OrderedFlag]
					,[PendingFlag]
					,[DeadFlag]
					,[ServiceFlag]
					,[SoldFlag])
 
				SELECT 
					[PKCommonDealId] 
					,[CRMDealID] 
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
					,[ProposalDealFlag] 
					,[InactiveDealFlag] 
					,[DateModified] 
					,[SoldNotDeliveredFlag] 
					,[OrderedFlag] 
					,[PendingFlag] 
					,[DeadFlag] 
					,[ServiceFlag] 
					,[SoldFlag]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonDeal] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonDeal] OFF;'
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  COMMONDEALLOG
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonDealLog')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonDealLog';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonDealLog] ON;

				INSERT INTO [_Common].[dbo].[CommonDealLog]
					([PKCommonDealLogId]
					,[FKCustomerID]
					,[DealLogDateCreated]
					,[FKImportLogID]
					,[FKUserIDSales1]
					,[FKDealID]
					,[DealLogType]
					,[DateModified])
      
				SELECT 
					[PKCommonDealLogId]
					,[FKCustomerID] 
					,[DealLogDateCreated] 
					,[FKImportLogID] 
					,[FKUserIDSales1] 
					,[FKDealID] 
					,[DealLogType] 
					,[DateModified]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonDealLog] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonDealLog] OFF;'
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  COMMONTASK
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonTask')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonTask';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonTask] ON;

				INSERT INTO [_Common].[dbo].[CommonTask]
					([PKCommonTaskId]
					,[FKImportLogID]
					,[FKCustomerID]
					,[FKUserID]
					,[FKCreatedByUserID]
					,[FKCompletedByUserID]
					,[FKDealID]
					,[TaskDateCreated]
					,[TaskDateModified]
					,[TaskDateCompleted]
					,[Subject]
					,[Description]
					,[Resolution]
					,[DateStart]
					,[DateDue]
					,[ResultType]
					,[DateModified])

 
				SELECT 
					[PKCommonTaskId] 
					,[FKImportLogID] 
					,[FKCustomerID] 
					,[FKUserID] 
					,[FKCreatedByUserID] 
					,[FKCompletedByUserID] 
					,[FKDealID] 
					,[TaskDateCreated]
					,[TaskDateModified] 
					,[TaskDateCompleted]
					,[Subject] 
					,[Description] 
					,[Resolution]
					,[DateStart] 
					,[DateDue] 
					,[ResultType] 
					,[DateModified]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonTask] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonTask] OFF;'
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  COMMONUSER
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CopyCommon', 'CommonUser')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CommonUser';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
				
			--ImportLog History
			INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
					([fkImportId]
					,[fkImportParentStageId]
					,[fkImportChildStageID]
					,[StageStart]
					,[StageEnd]
					,[RowCount]
					,[TableName]
					,[TableStartCount])
			SELECT	@ImportID
					,@ParentStage
					,@ChildStage
					,CURRENT_TIMESTAMP
					,NULL
					,NULL
					,@LeadcrumbTable
					,@LeadcrumbTableStartCount;

		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION;

				SET @sqlcmd = N'

				SET IDENTITY_INSERT [_Common].[dbo].[CommonUser] ON;

				INSERT INTO [_Common].[dbo].[CommonUser]
					([PKCommonUserId]
					,[CRMUserID]
					,[UserDateCreated]
					,[FKImportLogID]
					,[FKUserTypeID]
					,[FirstName]
					,[LastName]
					,[DateModified]
					,[BDCUser])
 
				SELECT 
					[PKCommonUserId] 
					,[CRMUserID]
					,[UserDateCreated] 
					,[FKImportLogID] 
					,[FKUserTypeID]
					,[FirstName] 
					,[LastName] 
					,[DateModified]
					,[BDCUser]
				FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[CommonUser] 
				WHERE fkImportLogID IN (SELECT TOP 1 pkImportID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].ImportLog WHERE fkStoreID = '+ @StoreID +' ORDER BY DateCreated DESC)

				SET @RowCountDynamic = @@ROWCOUNT

				SET IDENTITY_INSERT [_Common].[dbo].[CommonUser] OFF;'
				
				IF @Debug = 1
					PRINT @sqlcmd
				ELSE
					EXEC sp_executesql @sqlcmd, N'@RowCountDynamic INT OUT', @Rows OUT;
		COMMIT;
		
			END TRY
			BEGIN CATCH
				IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

				INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
					,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
					,@ParentStage
					,@ChildStage
					,GETDATE()
					,ERROR_NUMBER() 
					,ERROR_SEVERITY()
					,ERROR_STATE() 
					,ERROR_PROCEDURE() 
					,ERROR_LINE()
					,ERROR_MESSAGE();

				RETURN;
			END CATCH;
		END;

		UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @Rows,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
			AND StageEnd IS NULL
			AND fkImportId = @ImportID
			AND fkImportParentStageId = @ParentStage
			AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
	END

------------------------------------
--  END OF SCRIPT SUCCESS MESSAGE
------------------------------------
SET @ChildStageName = 'CopyCommon Successfully Completed';

	--Pull Child Stage ID
	SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);


	--ImportLog History
	INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
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
		PRINT ''CopyCommon has completed successfully.  Please check ImportHistory and ImportError for details'''


		IF @Debug = 1
			PRINT @sqlcmd
		ELSE
			EXEC sp_executesql @sqlcmd
			SET @Rows = @@ROWCOUNT;
COMMIT;
		
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0  
		ROLLBACK TRANSACTION;  

		INSERT INTO [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportErrorLog]
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
			,(SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage)
			,@ParentStage
			,@ChildStage
			,GETDATE()
			,ERROR_NUMBER() 
			,ERROR_SEVERITY()
			,ERROR_STATE() 
			,ERROR_PROCEDURE() 
			,ERROR_LINE()
			,ERROR_MESSAGE();

		RETURN;
	END CATCH;
END;

UPDATE [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory]
SET [RowCount] = @Rows,
	[StageEnd] = CURRENT_TIMESTAMP
WHERE [RowCount] IS NULL
	AND StageEnd IS NULL
	AND fkImportId = @ImportID
	AND fkImportParentStageId = @ParentStage
	AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
