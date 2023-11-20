SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[Index_ELeads]   
	@GUID UNIQUEIDENTIFIER

AS


/*

ELeads Staging Data - Indexes INITIAL DATA (CMS001) 

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
		@ParentStage INT = (SELECT pkImportParentStageID FROM [StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'Indexes_ELeads'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(300),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@ErrorMessage  NVARCHAR(4000), 
		@ErrorSeverity INT, 
		@ErrorState    INT,
		@CompletionCheck BIT;




------------------------------------
--   CREATE CLUSTERED INDEX cix_iActivities_TaskID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_ELeads', 'CREATE CLUSTERED INDEX cix_iActivities_TaskID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cix_iActivities_TaskID';

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


			BEGIN TRY
		 

				SET @sqlcmd = N'

					DROP INDEX IF EXISTS cix_iActivities_TaskID on ' + @TableLocation + '_iActivities'+ @DynamicStoreID +'

					CREATE CLUSTERED INDEX cix_iActivities_TaskID on ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' (lTaskID) '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
 
		
			END TRY
			BEGIN CATCH
		   
		   

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
-- CREATE CLUSTERED INDEX cix_iEmailBody_messagedID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_ELeads', 'CREATE CLUSTERED INDEX cix_iEmailBody_messagedID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cix_iEmailBody_messagedID';

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


			BEGIN TRY
		 

				SET @sqlcmd = N'
		
					DROP INDEX IF EXISTS cix_iEmailBody_messagedID ON ' + @TableLocation + '_iEmailBody'+ @DynamicStoreID +'
			
					CREATE CLUSTERED INDEX cix_iEmailBody_messagedID ON ' + @TableLocation + '_iEmailBody'+ @DynamicStoreID +' (lMessageID) '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
 
		
			END TRY
			BEGIN CATCH
		   
		   

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
-- CREATE CLUSTERED INDEX cix_iMessages_TaskID_MessageID 
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_ELeads', 'CREATE CLUSTERED INDEX cix_iMessages_TaskID_MessageID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cix_iMessages_TaskID_MessageID';

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


			BEGIN TRY
		 

				SET @sqlcmd = N'
		
					DROP INDEX IF EXISTS cix_iMessages_TaskID_MessageID on ' + @TableLocation + '_iMessages'+ @DynamicStoreID +'
			
					CREATE CLUSTERED INDEX cix_iMessages_TaskID_MessageID on ' + @TableLocation + '_iMessages'+ @DynamicStoreID +' (lTaskID,lMessageID) '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
 
		
			END TRY
			BEGIN CATCH
		   
		   

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
-- CREATE CLUSTERED INDEX cix_iDeals_lDealID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_ELeads', 'CREATE CLUSTERED INDEX cix_iDeals_lDealID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cix_iDeals_lDealID';

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


			BEGIN TRY
		 

				SET @sqlcmd = N'
		
					DROP INDEX IF EXISTS cix_iDeals_lDealID ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
			
					CREATE CLUSTERED INDEX cix_iDeals_lDealID ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +' (lDealID) '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
 
		
			END TRY
			BEGIN CATCH
		   
		   

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
-- CREATE CLUSTERED INDEX cix_iVehicles_lDealID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_ELeads', 'CREATE CLUSTERED INDEX cix_iVehicles_lDealID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cix_iVehicles_lDealID';

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


			BEGIN TRY
		 

				SET @sqlcmd = N'

					DROP INDEX IF EXISTS cix_iVehicles_lDealID ON ' + @TableLocation + '_iVehicles'+ @DynamicStoreID +'
		
					CREATE CLUSTERED INDEX cix_iVehicles_lDealID ON ' + @TableLocation + '_iVehicles'+ @DynamicStoreID +' (lDealID) '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
 
		
			END TRY
			BEGIN CATCH
		   
		   

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
-- CREATE CLUSTERED INDEX cix_iDealUser_lDealID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_ELeads', 'CREATE CLUSTERED INDEX cix_iDealUser_lDealID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cix_iDealUser_lDealID';

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


			BEGIN TRY
		 

				SET @sqlcmd = N'
		
					DROP INDEX IF EXISTS cix_iDealUser_lDealID ON ' + @TableLocation + '_iDealUser'+ @DynamicStoreID +'
			
					CREATE CLUSTERED INDEX cix_iDealUser_lDealID ON ' + @TableLocation + '_iDealUser'+ @DynamicStoreID +' (lDealID)
		 '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
 
		
			END TRY
			BEGIN CATCH
		   
		   

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
-- CREATE CLUSTERED INDEX cix_iTaskItem_szListItem_lDealID 
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_ELeads', 'CREATE CLUSTERED INDEX cix_iTaskItem_szListItem_lDealID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cix_iTaskItem_szListItem_lDealID';

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

			BEGIN TRY
		 

				SET @sqlcmd = N'
		
					DROP INDEX IF EXISTS cix_iTaskItem_szListItem_lDealID ON ' + @TableLocation + '_iTaskItem'+ @DynamicStoreID +'
			
					CREATE CLUSTERED INDEX cix_iTaskItem_szListItem_lDealID ON ' + @TableLocation + '_iTaskItem'+ @DynamicStoreID +' (szListItem, lDealID)  '


				EXEC sp_executesql @sqlcmd
				SET @Rows = @@ROWCOUNT;
 
		
			END TRY
			BEGIN CATCH
		   
		   

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
SET @ChildStageName = 'Indexes_ELeads Successfully Completed';

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


	BEGIN TRY
		 

		SET @sqlcmd = N'
		PRINT ''Eleads Indexes has completed successfully.  Please check ImportHistory and ImportError for details'''

			EXEC sp_executesql @sqlcmd
			SET @Rows = @@ROWCOUNT;
 
		
	END TRY
	BEGIN CATCH
		   
		   

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
 

UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
SET [RowCount] = @Rows,
	[StageEnd] = CURRENT_TIMESTAMP
WHERE [RowCount] IS NULL
	AND StageEnd IS NULL
	AND fkImportId = @ImportID
	AND fkImportParentStageId = @ParentStage
	AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);

