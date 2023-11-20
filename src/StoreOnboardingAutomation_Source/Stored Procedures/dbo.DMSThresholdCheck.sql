SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[DMSThresholdCheck]   
	@GUID UNIQUEIDENTIFIER

AS

/* 

NEW ONBOARDING PROCESS

1.5  DMS Threshold Check -  Run on Leadcrumb server 


SELECT * FROM [StoreOnboardingAutomation].dbo.ImportLog ORDER BY DateCreated DESC
SELECT * FROM [StoreOnboardingAutomation].DmsImportSourceConfig


*/

DECLARE @ImportID VARCHAR(MAX)
DECLARE @StoreID VARCHAR(5) 
DECLARE @IP VARCHAR(50)
DECLARE @leadcrumb VARCHAR(50)
DECLARE @DMS VARCHAR(50)

SET NOCOUNT ON

SELECT 
@ImportID = pkImportID,
@StoreID = FKStoreID,
@DMS = sc.[Name]
FROM [StoreOnboardingAutomation].[dbo].[ImportLog] il
INNER JOIN [StoreOnboardingAutomation].[dbo].DmsImportSourceConfig sc
	ON il.FKDMSID = sc.DmsImportSourceConfigId
WHERE GUID = @GUID
	AND (DateCompleted = '' OR DateCompleted IS NULL)

--SELECT @ImportID, @StoreID, @DMS

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


-------------------------------------------------------------------
-- VARIABLES
-------------------------------------------------------------------

DECLARE @YearCount INT,
		@DeliveryCount INT,
		@DMSDeliveryCount INT,
		@ADPVehicleSaleCount INT,
		@ArkonaDealCount INT,
		@ReynoldsDealCount INT,
		@ErrorCheck INT,
		@ParentStage INT = (SELECT pkImportParentStageID FROM [StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'DMS Threshold Check'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(100),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@DMSMinimumYear VARCHAR(10) = '3',
		@DMSMinimumDelivery VARCHAR(10) = '500',
		@DMSAllowedVariation INT = 10,
		@strErr VARCHAR(500),
		@ErrorMessage  NVARCHAR(4000), 
		@ErrorSeverity INT, 
		@ErrorState    INT;


------------------------------------
--CDK ATLAS
------------------------------------
IF @DMS = 'CDK (Atlas)'
	BEGIN
		
		------------------------------------
		--DMS MINIMUM YEAR CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Year Check';

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
				SELECT @YearCountDynamic = COUNT( DISTINCT YEAR(DateDelivered) ) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].DmsDelivery WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@YearCountDynamic INT OUT', @YearCount OUT
		END TRY

		BEGIN CATCH
			IF @YearCount IS NULL
		
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
				
		IF @YearCount < @DMSMinimumYear
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumYear +' minimum years of DMS Data';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			END;


		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @YearCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);

		------------------------------------
		--DMS MINIMUM DELIVERIES CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Deliveries Check';

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
				SELECT @DeliveryCountDynamic = COUNT(*) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].DmsDelivery WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@DeliveryCountDynamic INT OUT', @DeliveryCount OUT
		END TRY

		BEGIN CATCH
			IF @DeliveryCount IS NULL
		
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


		IF  @DeliveryCount/@YearCount < @DMSMinimumDelivery
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumDelivery +' minimum delivery per year of DMS Data ';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			 END;
			 		

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @DeliveryCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);
				

		------------------------------------
		--  END OF SCRIPT SUCCESS MESSAGE
		------------------------------------
		SET @ChildStageName = 'DMS Threshold Check Successfully Completed';

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
				,CURRENT_TIMESTAMP
				,0;

		
		SET @sqlcmd = N'
		PRINT ''DMS Threshold Check has completed successfully.  Please check ImportHistory and ImportError for details'''

		EXEC sp_executesql @sqlcmd
				
	END

------------------------------------
--DEALERTRACK ATLAS
------------------------------------
IF @DMS = 'DealerTrack (Atlas)'
	BEGIN
	
		------------------------------------
		--DMS MINIMUM YEAR CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Year Check';

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
				SELECT @YearCountDynamic = COUNT( DISTINCT YEAR(DateDelivered) ) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].DmsDelivery WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@YearCountDynamic INT OUT', @YearCount OUT
		END TRY

		BEGIN CATCH
			IF @YearCount IS NULL
		
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
				
		IF @YearCount < @DMSMinimumYear
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumYear +' minimum years of DMS Data';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			END;


		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @YearCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);

		------------------------------------
		--DMS MINIMUM DELIVERIES CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Deliveries Check';

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
				SELECT @DeliveryCountDynamic = COUNT(*) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].DmsDelivery WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@DeliveryCountDynamic INT OUT', @DeliveryCount OUT
		END TRY

		BEGIN CATCH
			IF @DeliveryCount IS NULL
		
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

		IF  @DeliveryCount/@YearCount < @DMSMinimumDelivery
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumDelivery +' minimum delivery per year of DMS Data ';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			 END;
			 		

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @DeliveryCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);


		------------------------------------
		--  END OF SCRIPT SUCCESS MESSAGE
		------------------------------------
		SET @ChildStageName = 'DMS Threshold Check Successfully Completed';

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
				,CURRENT_TIMESTAMP
				,0;

		
		SET @sqlcmd = N'
		PRINT ''DMS Threshold Check has completed successfully.  Please check ImportHistory and ImportError for details'''

		EXEC sp_executesql @sqlcmd

	END

------------------------------------
--REYNOLDS ATLAS
------------------------------------
IF @DMS = 'Reynolds (Atlas)'
	BEGIN
	
		------------------------------------
		--DMS MINIMUM YEAR CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Year Check';

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
				SELECT @YearCountDynamic = COUNT( DISTINCT YEAR(DateDelivered) ) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].DmsDelivery WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@YearCountDynamic INT OUT', @YearCount OUT
		END TRY

		BEGIN CATCH
			IF @YearCount IS NULL
		
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
				
		IF @YearCount < @DMSMinimumYear
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumYear +' minimum years of DMS Data';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			END;


		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @YearCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);

		------------------------------------
		--DMS MINIMUM DELIVERIES CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Deliveries Check';

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
				SELECT @DeliveryCountDynamic = COUNT(*) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].DmsDelivery WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@DeliveryCountDynamic INT OUT', @DeliveryCount OUT
		END TRY

		BEGIN CATCH
			IF @DeliveryCount IS NULL
		
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

		IF  @DeliveryCount/@YearCount < @DMSMinimumDelivery
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumDelivery +' minimum delivery per year of DMS Data ';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			 END;
			 		

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @DeliveryCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);

		
		------------------------------------
		--  END OF SCRIPT SUCCESS MESSAGE
		------------------------------------
		SET @ChildStageName = 'DMS Threshold Check Successfully Completed';

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
				,CURRENT_TIMESTAMP
				,0;

		
		SET @sqlcmd = N'
		PRINT ''DMS Threshold Check has completed successfully.  Please check ImportHistory and ImportError for details'''

		EXEC sp_executesql @sqlcmd

	END


------------------------------------
--REYNOLDS
------------------------------------
IF @DMS = 'Reynolds'	
	BEGIN
	
		------------------------------------
		--DMS MINIMUM YEAR CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Year Check';

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
				SELECT @YearCountDynamic = COUNT( DISTINCT YEAR(DeliveryDateConverted) ) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].ReynoldsDeal WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@YearCountDynamic INT OUT', @YearCount OUT
		END TRY

		BEGIN CATCH
			IF @YearCount IS NULL
		
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
				
		IF @YearCount < @DMSMinimumYear
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumYear +' minimum years of DMS Data';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			END;


		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @YearCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);

		------------------------------------
		--DMS MINIMUM DELIVERIES CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Deliveries Check';

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
				SELECT @DeliveryCountDynamic = COUNT(*) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].ReynoldsDeal WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@DeliveryCountDynamic INT OUT', @DeliveryCount OUT
		END TRY

		BEGIN CATCH
			IF @DeliveryCount IS NULL
		
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

		IF  @DeliveryCount/@YearCount < @DMSMinimumDelivery
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumDelivery +' minimum delivery per year of DMS Data ';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			 END;
			 		

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @DeliveryCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);		

		------------------------------------
		--  END OF SCRIPT SUCCESS MESSAGE
		------------------------------------
		SET @ChildStageName = 'DMS Threshold Check Successfully Completed';

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
				,CURRENT_TIMESTAMP
				,0;

		
		SET @sqlcmd = N'
		PRINT ''DMS Threshold Check has completed successfully.  Please check ImportHistory and ImportError for details'''

		EXEC sp_executesql @sqlcmd

	END

------------------------------------------
--AUTOMATE, PBS, AUTOSOFT, DOMINION ATLAS
------------------------------------------
IF @DMS IN ('Automate','PBS','AutoSoft','Dominion (Atlas)')	
	BEGIN

		------------------------------------
		--DMS MINIMUM YEAR CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Year Check';

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
				SELECT @YearCountDynamic = COUNT( DISTINCT YEAR(DateDelivered) ) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].DMSDelivery WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@YearCountDynamic INT OUT', @YearCount OUT
		END TRY

		BEGIN CATCH
			IF @YearCount IS NULL
		
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
		
		IF @YearCount < @DMSMinimumYear
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumYear +' minimum years of DMS Data';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			END;


		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @YearCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);

		------------------------------------
		--DMS MINIMUM DELIVERIES CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Deliveries Check';

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
				SELECT @DeliveryCountDynamic = COUNT(*) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].DMSDelivery WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@DeliveryCountDynamic INT OUT', @DeliveryCount OUT
		END TRY

		BEGIN CATCH
			IF @DeliveryCount IS NULL
		
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

		IF  @DeliveryCount/@YearCount < @DMSMinimumDelivery
			BEGIN

				 SET @strErr = 'Error: There are less than '+ @DMSMinimumDelivery +' minimum delivery per year of DMS Data ';

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
					,@strErr;

				 RAISERROR(@strErr, 16, 1);
				 RETURN;

			 END;
			 		

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @DeliveryCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);		

		------------------------------------
		--  END OF SCRIPT SUCCESS MESSAGE
		------------------------------------
		SET @ChildStageName = 'DMS Threshold Check Successfully Completed';

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
				,CURRENT_TIMESTAMP
				,0;

		
		SET @sqlcmd = N'
		PRINT ''DMS Threshold Check has completed successfully.  Please check ImportHistory and ImportError for details'''

		EXEC sp_executesql @sqlcmd

	END

------------------------------------
--DEALERBUILT
------------------------------------
IF @DMS IN ('Dealerbuilt')	
	BEGIN

		------------------------------------
		--DMS MINIMUM YEAR CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Year Check';

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
				SELECT @YearCountDynamic = COUNT( DISTINCT YEAR(DateDelivered) ) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].UnLinkedDeal WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@YearCountDynamic INT OUT', @YearCount OUT
		END TRY

		BEGIN CATCH
			IF @YearCount IS NULL
		
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

		IF @YearCount < @DMSMinimumYear
			BEGIN

					SET @strErr = 'Error: There are less than '+ @DMSMinimumYear +' minimum years of DMS Data';

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
					,@strErr;

					RAISERROR(@strErr, 16, 1);
					RETURN;

			END;


		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @YearCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);

		------------------------------------
		--DMS MINIMUM DELIVERIES CHECK
		------------------------------------
		SET @ChildStageName = 'Minimum Deliveries Check';

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
				SELECT @DeliveryCountDynamic = COUNT(*) FROM [' + @IP + '].[' + @leadcrumb + '].[dbo].UnLinkedDeal WITH (NOLOCK) WHERE fkStoreID = '+ @StoreID
			EXEC sp_executesql @sqlcmd, N'@DeliveryCountDynamic INT OUT', @DeliveryCount OUT
		END TRY

		BEGIN CATCH
			IF @DeliveryCount IS NULL
		
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

		IF  @DeliveryCount/@YearCount < @DMSMinimumDelivery
			BEGIN

					SET @strErr = 'Error: There are less than '+ @DMSMinimumDelivery +' minimum delivery per year of DMS Data ';

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
					,@strErr;

					RAISERROR(@strErr, 16, 1);
					RETURN;

				END;
			 		

		UPDATE [StoreOnboardingAutomation].[dbo].[ImportHistory]
		SET [RowCount] = @DeliveryCount,
			[StageEnd] = CURRENT_TIMESTAMP
		WHERE [RowCount] IS NULL
		AND StageEnd IS NULL
		AND fkImportId = @ImportID
		AND fkImportParentStageId = @ParentStage
		AND PKImportHistoryId = (SELECT MAX(PKImportHistoryId) FROM [StoreOnboardingAutomation].[dbo].[ImportHistory] WHERE fkImportId = @ImportID AND fkImportChildStageID = @ChildStage);		

		------------------------------------
		--  END OF SCRIPT SUCCESS MESSAGE
		------------------------------------
		SET @ChildStageName = 'DMS Threshold Check Successfully Completed';

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
				,CURRENT_TIMESTAMP
				,0;

		
		SET @sqlcmd = N'
		PRINT ''DMS Threshold Check has completed successfully.  Please check ImportHistory and ImportError for details'''

		EXEC sp_executesql @sqlcmd

	END


