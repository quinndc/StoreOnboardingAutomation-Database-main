SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[Cleanup_ELeads]
	@GUID UNIQUEIDENTIFIER

AS

/*
ELeads Staging Data - Clean and Transform INITIAL DATA (CMS001) 
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
WHERE GUID = @GUID
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
		@ParentStage INT = (SELECT pkImportParentStageID FROM [StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'StagingCleanup_ELeads'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(300),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@ErrorMessage  NVARCHAR(4000), 
		@ErrorSeverity INT, 
		@ErrorState    INT;		


------------------------------------
--  FIX NON NUMERIC PHONE NUMBERS
------------------------------------
SET @ChildStageName  = 'Fix Non Numeric Phone Numbers';

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

			UPDATE ' + @TableLocation + '_iPhones' + @DynamicStoreID + '
			SET szNumber = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(szNumber, ''('', ''''), '')'', ''''), '' '', ''''), ''-'', ''''), ''*'', ''''), 
				szAreaCode = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(szAreaCode, ''('', ''''), '')'', ''''), '' '', ''''), ''-'', ''''), ''*'', '''') '


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


------------------------------------
--  NUKE EMPTY CUSTOMERS
------------------------------------
SET @ChildStageName  = 'Nuke Empty Customers';

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


			DELETE FROM ' + @TableLocation + '_iCustomers' + @DynamicStoreID + '
			WHERE ISNULL([szFirstName], '''') = '''' 
				AND ISNULL([szLastName], '''') = '''' '

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


------------------------------------
--  NUKE TXT PHONES WITHOUT SENDER
------------------------------------
SET @ChildStageName  = 'Nuke Text Phones Without Sender';

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

			DELETE FROM ' + @TableLocation + '_iTexts' + @DynamicStoreID + '
			WHERE ISNULL([TextSender], '''') = '''' 
					AND ISNULL([TextSender], '''') = '''' '
		

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



------------------------------------
--  DELETE FROM _iActivities
------------------------------------
SET @ChildStageName  = 'DELETE FROM _iActivities';

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

			DELETE FROM ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' WHERE ISNUMERIC(lTaskID) = 0 and lTaskID IS NOT NULL '

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



------------------------------------
--  DELETE FROM _iEmailBody
------------------------------------
SET @ChildStageName  = 'DELETE FROM _iEmailBody';

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

			DELETE FROM ' + @TableLocation + '_iEmailBody'+ @DynamicStoreID +' WHERE ISNUMERIC(lMessageID) = 0 and lMessageID IS NOT NULL '

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



	
------------------------------------
--  DELETE FROM _iMessages (lMessageID)
------------------------------------
SET @ChildStageName  = 'DELETE FROM _iMessages (lMessageID)';

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

			DELETE FROM ' + @TableLocation + '_iMessages'+ @DynamicStoreID +' WHERE ISNUMERIC(lMessageID) = 0 and lMessageID IS NOT NULL  '

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





------------------------------------
--  DELETE FROM _iMessages (lTaskID)
------------------------------------
SET @ChildStageName  = 'DELETE FROM _iMessages (lTaskID)';

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

			DELETE FROM ' + @TableLocation + '_iMessages'+ @DynamicStoreID +' WHERE ISNUMERIC(lTaskID) = 0 and lTaskID IS NOT NULL  '

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


	
------------------------------------
--  ALTER TABLE _iActivities (lTaskID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iActivities (lTaskID)';

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

			ALTER TABLE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' ALTER COLUMN lTaskID  VARCHAR(32)  '

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


	
------------------------------------
--  ALTER TABLE _iEmailBody (lMessageID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iEmailBody (lMessageID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iEmailBody'+ @DynamicStoreID +'  ALTER COLUMN lMessageID VARCHAR(32) '

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


	
------------------------------------
--  ALTER TABLE _iMessages (lMessageID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iMessages (lMessageID)';

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

			ALTER TABLE ' + @TableLocation + '_iMessages'+ @DynamicStoreID +'   ALTER COLUMN lMessageID VARCHAR(32) '

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


------------------------------------
--  ALTER TABLE _iMessages (lTaskID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iMessages (lTaskID)';

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

			ALTER TABLE ' + @TableLocation + '_iMessages'+ @DynamicStoreID +'   ALTER COLUMN lTaskID    VARCHAR(32) '

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


------------------------------------
--  ALTER TABLE _iCustomers (lPersonID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iCustomers (lPersonID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iCustomers'+ @DynamicStoreID +' ALTER COLUMN lPersonID VARCHAR(2048) '

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


	
------------------------------------
--  ALTER TABLE _iDeals (lPersonID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iDeals (lPersonID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iDeals'+ @DynamicStoreID +' ALTER COLUMN lPersonID VARCHAR(2048)  '

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


	
------------------------------------
--  ALTER TABLE _iEmails (lPersonID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iEmails (lPersonID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iEmails'+ @DynamicStoreID +' ALTER COLUMN lPersonID VARCHAR(2048) '

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


	
------------------------------------
--  ALTER TABLE _iPhones (lPersonID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iPhones (lPersonID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iPhones'+ @DynamicStoreID +' ALTER COLUMN lPersonID VARCHAR(2048) '

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


	
------------------------------------
--  ALTER TABLE _iActivities (lCustomerID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iActivities (lCustomerID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' ALTER COLUMN lCustomerID VARCHAR(2048) '

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


	
------------------------------------
--  ALTER TABLE _iDeals (lDealID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iDeals (lDealID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iDeals'+ @DynamicStoreID +' ALTER COLUMN lDealID VARCHAR(256)    '

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


	
------------------------------------
--  ALTER TABLE _iVehicles (lDealID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iVehicles (lDealID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iVehicles'+ @DynamicStoreID +' ALTER COLUMN lDealID VARCHAR(256) '

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


	
------------------------------------
--  ALTER TABLE _iActivities (lDealID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iActivities (lDealID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' ALTER COLUMN lDealID VARCHAR(256)  '

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


	
------------------------------------
--  ALTER TABLE _iDealUser (lDealID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iDealUser (lDealID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iDealUser'+ @DynamicStoreID +' ALTER COLUMN lDealID VARCHAR(256)  '

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


	
------------------------------------
--  ALTER TABLE _iTaskItem (lDealID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iTaskItem (lDealID)';

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
		
		ALTER TABLE ' + @TableLocation + '_itaskitem'+ @DynamicStoreID +' ALTER COLUMN lDealID VARCHAR(256) '

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


	
------------------------------------
--  ALTER TABLE _iTaskItem (szListItem)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iTaskItem (szListItem)';

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
		
		ALTER TABLE ' + @TableLocation + '_itaskitem'+ @DynamicStoreID +' ALTER COLUMN szListItem VARCHAR(256) '

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


	
------------------------------------
--  ALTER TABLE _iDealUser (lSalespersonID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iDealUser (lSalespersonID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iDealUser'+ @DynamicStoreID +' ALTER COLUMN lSalespersonID VARCHAR(8000)  '

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


	
------------------------------------
--  ALTER TABLE _iActivities (lCompletedByID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iActivities (lCompletedByID)';

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
		
		ALTER TABLE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' ALTER COLUMN lCompletedByID VARCHAR(8000)    '

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


	
------------------------------------
--  UPDATE _iActivities (dtActive)
------------------------------------
SET @ChildStageName  = 'UPDATE _iActivities (dtActive)';

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

			UPDATE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' SET dtActive = LEFT(dtActive,23) WHERE LEN(dtActive) = 29  '

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


	
------------------------------------
--  UPDATE _iActivities (dtDue)
------------------------------------
SET @ChildStageName  = 'UPDATE _iActivities (dtDue)';

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

			UPDATE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' SET dtDue = LEFT(dtDue,23) WHERE LEN(dtDue) = 29  '

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


	
------------------------------------
--  UPDATE _iActivities (dtSentToCallCenter)
------------------------------------
SET @ChildStageName  = 'UPDATE _iActivities (dtSentToCallCenter)';

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

			UPDATE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' SET dtSentToCallCenter = LEFT(dtSentToCallCenter,23) WHERE LEN(dtSentToCallCenter) = 29  '

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


	
------------------------------------
--  UPDATE _iActivities (dtCompleted)
------------------------------------
SET @ChildStageName  = 'UPDATE _iActivities (dtCompleted)';

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

			UPDATE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' SET dtCompleted = LEFT(dtCompleted,23) WHERE LEN(dtCompleted) = 29 '

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


	
------------------------------------
--  UPDATE _iActivities (dtClosed)
------------------------------------
SET @ChildStageName  = 'UPDATE _iActivities (dtClosed)';

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

			UPDATE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' SET dtClosed = LEFT(dtClosed,23) WHERE LEN(dtClosed) = 29  '

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


	
------------------------------------
--  UPDATE _iActivities (dtEntry)
------------------------------------
SET @ChildStageName  = 'UPDATE _iActivities (dtEntry)';

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

			UPDATE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' SET dtEntry = LEFT(dtEntry,23) WHERE LEN(dtEntry) = 29  '

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


	
------------------------------------
--  UPDATE _iActivities (dtLastEdit)
------------------------------------
SET @ChildStageName  = 'UPDATE _iActivities (dtLastEdit)';

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

			UPDATE ' + @TableLocation + '_iActivities'+ @DynamicStoreID +' SET dtLastEdit = LEFT(dtLastEdit,23) WHERE LEN(dtLastEdit) = 29 '

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


	
------------------------------------
--  UPDATE _iMessages (dtEntry)
------------------------------------
SET @ChildStageName  = 'UPDATE _iMessages (dtEntry)';

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

			UPDATE ' + @TableLocation + '_iMessages'+ @DynamicStoreID +'   SET dtEntry = LEFT(dtEntry,23) WHERE LEN(dtEntry) = 29  '

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




------------------------------------
--  END OF SCRIPT SUCCESS MESSAGE
------------------------------------
SET @ChildStageName  = 'StagingCleanup_Eleads Successfully Completed';

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
		PRINT ''ELeads Staging Data - Clean and Transform has completed successfully.  Please check ImportHistory and ImportError for details'''

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

