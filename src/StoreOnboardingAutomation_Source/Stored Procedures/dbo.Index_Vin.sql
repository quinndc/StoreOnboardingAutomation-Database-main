SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[Index_Vin]   
	@GUID UNIQUEIDENTIFIER

AS


/*

Vin Staging Data - Indexes INITIAL DATA (CMS001) 

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
		@ParentStage INT = (SELECT pkImportParentStageID FROM [StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'Indexes_Vin'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(300),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@ErrorMessage  NVARCHAR(4000), 
		@ErrorSeverity INT, 
		@ErrorState    INT,
		@CompletionCheck BIT;



------------------------------------
--   CREATE CLUSTERED INDEX cidx_iCustomers_IdValues
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE CLUSTERED INDEX cidx_iCustomers_IdValues')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cidx_iCustomers_IdValues';

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

					DROP INDEX IF EXISTS cidx_iCustomers_IdValues ON ' + @TableLocation + '_iCustomers'+ @DynamicStoreID +'

					CREATE CLUSTERED INDEX cidx_iCustomers_IdValues ON ' + @TableLocation + '_iCustomers'+ @DynamicStoreID +' (GlobalCustomerID,CurrentSalesRepUserID) '


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
-- CCREATE NONCLUSTERED INDEX idx_iCustomers_CurrentSalesRepUserID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE NONCLUSTERED INDEX idx_iCustomers_CurrentSalesRepUserID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE NONCLUSTERED INDEX idx_iCustomers_CurrentSalesRepUserID';

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
		
					DROP INDEX IF EXISTS idx_iCustomers_CurrentSalesRepUserID ON ' + @TableLocation + '_iCustomers'+ @DynamicStoreID +'
			
					CREATE NONCLUSTERED INDEX idx_iCustomers_CurrentSalesRepUserID ON ' + @TableLocation + '_iCustomers'+ @DynamicStoreID +' (CurrentSalesRepUserID) '


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
-- CREATE CLUSTERED INDEX cidx_iDeals_AutoLeadID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE CLUSTERED INDEX cidx_iDeals_AutoLeadID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cidx_iDeals_AutoLeadID';

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
		
					DROP INDEX IF EXISTS cidx_iDeals_AutoLeadID ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
			
					CREATE CLUSTERED INDEX cidx_iDeals_AutoLeadID ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +' (AutoLeadID) '


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
-- CREATE NONCLUSTERED INDEX idx_iDeals_IdValues
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE NONCLUSTERED INDEX idx_iDeals_IdValues')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE NONCLUSTERED INDEX idx_iDeals_IdValues';

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
		
					DROP INDEX IF EXISTS idx_iDeals_IdValues ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
			
					CREATE NONCLUSTERED INDEX idx_iDeals_IdValues ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +' (GlobalCustomerID,CoBuyerGlobalCustomerID,AutoLeadID) '


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
-- CREATE NONCLUSTERED INDEX idx_iDeals_MiscIDs
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE NONCLUSTERED INDEX idx_iDeals_MiscIDs')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE NONCLUSTERED INDEX idx_iDeals_MiscIDs';

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

					DROP INDEX IF EXISTS idx_iDeals_MiscIDs ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
		
					CREATE NONCLUSTERED INDEX idx_iDeals_MiscIDs ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +' (CoBuyerGlobalCustomerID,AutoLeadID) '


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
-- CREATE NONCLUSTERED INDEX idx_iDeals_CreatedByUserID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE NONCLUSTERED INDEX idx_iDeals_CreatedByUserID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE NONCLUSTERED INDEX idx_iDeals_CreatedByUserID';

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
		
					DROP INDEX IF EXISTS idx_iDeals_CreatedByUserID ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
			
					CREATE NONCLUSTERED INDEX idx_iDeals_CreatedByUserID ON ' + @TableLocation + '_iDeals'+ @DynamicStoreID +' (CreatedByUserID)
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
-- CREATE CLUSTERED INDEX cidx_iNotes_LeadMessageTypeName
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE CLUSTERED INDEX cidx_iNotes_LeadMessageTypeName')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cidx_iNotes_LeadMessageTypeName';

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
		
					DROP INDEX IF EXISTS cidx_iNotes_LeadMessageTypeName ON ' + @TableLocation + '_iNotes'+ @DynamicStoreID +'
			
					CREATE CLUSTERED INDEX cidx_iNotes_LeadMessageTypeName ON ' + @TableLocation + '_iNotes'+ @DynamicStoreID +' (LeadMessageTypeName,AutoLeadID,CreatedByUserID)  '


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
-- CREATE NONCLUSTERED INDEX idx_iNotes_AutoLeadID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE NONCLUSTERED INDEX idx_iNotes_AutoLeadID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE NONCLUSTERED INDEX idx_iNotes_AutoLeadID';

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
		
					DROP INDEX IF EXISTS idx_iNotes_AutoLeadID ON ' + @TableLocation + '_iNotes'+ @DynamicStoreID +'
			
					CREATE NONCLUSTERED INDEX idx_iNotes_AutoLeadID ON ' + @TableLocation + '_iNotes'+ @DynamicStoreID +' (AutoLeadID,CreatedByUserID)  '


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
-- CREATE NONCLUSTERED INDEX idx_iNotes_CreatedByUserID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE NONCLUSTERED INDEX idx_iNotes_CreatedByUserID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE NONCLUSTERED INDEX idx_iNotes_CreatedByUserID';

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
		
					DROP INDEX IF EXISTS idx_iNotes_CreatedByUserID ON ' + @TableLocation + '_iNotes'+ @DynamicStoreID +'
			
					CREATE NONCLUSTERED INDEX idx_iNotes_CreatedByUserID ON ' + @TableLocation + '_iNotes'+ @DynamicStoreID +' (CreatedByUserID)  '


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
--  CREATE CLUSTERED INDEX cidx_iTrades_AutoLeadID
------------------------------------
SELECT @CompletionCheck = [StoreOnboardingAutomation].[dbo].fnIsImportSegmentCompleted (@GUID, 'Indexes_Vin', 'CREATE CLUSTERED INDEX cidx_iTrades_AutoLeadID')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'CREATE CLUSTERED INDEX cidx_iTrades_AutoLeadID';

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
		
					DROP INDEX IF EXISTS cidx_iTrades_AutoLeadID ON ' + @TableLocation + '_iTrades'+ @DynamicStoreID +'
			
					CREATE CLUSTERED INDEX cidx_iTrades_AutoLeadID ON ' + @TableLocation + '_iTrades'+ @DynamicStoreID +' (AutoLeadID) '  


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
SET @ChildStageName = 'Indexes_Vin Successfully Completed';

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
		PRINT ''Vin Indexes has completed successfully.  Please check ImportHistory and ImportError for details'''

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
