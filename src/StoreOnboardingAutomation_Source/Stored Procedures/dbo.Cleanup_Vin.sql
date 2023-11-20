SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[Cleanup_Vin] 
	@GUID UNIQUEIDENTIFIER

AS


/*
Vin Staging Data - Clean and Transform INITIAL DATA (CMS001)
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
		@ParentStage INT = (SELECT pkImportParentStageID FROM [StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'StagingCleanup_Vin'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(300),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@ErrorMessage  NVARCHAR(4000), 
		@ErrorSeverity INT, 
		@ErrorState    INT;



------------------------------------
-- CLEANUP CUSTOMER PHONES
------------------------------------
SET @ChildStageName  = 'CLEANUP CUSTOMER PHONES';

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

			UPDATE ' + @TableLocation + '_iCustomers' + @DynamicStoreID + '
		SET DayPhone = replace(replace(replace(replace(replace(DayPhone, ''('', ''''), '')'', ''''), '' '', ''''), ''-'', ''''), ''*'', ''''),
			EvePhone = replace(replace(replace(replace(replace(EvePhone, ''('', ''''), '')'', ''''), '' '', ''''), ''-'', ''''), ''*'', ''''),
			CellPhone = replace(replace(replace(replace(replace(CellPhone, ''('', ''''), '')'', ''''), '' '', ''''), ''-'', ''''), ''*'', '''') '


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
		WHERE ISNULL(firstname, '''') = '''' 
			AND ISNULL(lastname, '''') = '''' 
			AND ISNULL(companyname, '''') = '''' '

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
		WHERE ISNULL([SenderPhone], '''') = '''' 
				AND ISNULL([SenderPhone], '''') = '''' '
		

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
--  NUKE EMPTY TRADES
------------------------------------
SET @ChildStageName  = 'Nuke Empty Trades';

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

		DELETE FROM ' + @TableLocation + '_iTrades' + @DynamicStoreID + '
		WHERE ISNULL(Make, '''') = '''' '
		

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
--  CLEANUP TEXT PHONES
------------------------------------
SET @ChildStageName  = 'Cleanup Text Phones';

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

		UPDATE ' + @TableLocation + '_iTexts' + @DynamicStoreID + '
		SET SenderPhone = replace(replace(replace(replace(replace(replace(SenderPhone, ''('', ''''), '')'', ''''), '' '', ''''), ''-'', ''''), ''*'', ''''),''+'', ''''),
			ReceiverPhone = replace(replace(replace(replace(replace(replace(ReceiverPhone, ''('', ''''), '')'', ''''), '' '', ''''), ''-'', ''''), ''*'', ''''),''+'', '''') '
		

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
--  ALTER TABLE _iCustomers (GlobalCustomerID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iCustomers (GlobalCustomerID)';

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

		ALTER TABLE ' + @TableLocation + '_iCustomers'+ @DynamicStoreID +'
		ALTER COLUMN GlobalCustomerID varchar(32) '
		

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
--  ALTER TABLE _iCustomers (CurrentSalesRepUserID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iCustomers (CurrentSalesRepUserID)';

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

		ALTER TABLE ' + @TableLocation + '_iCustomers'+ @DynamicStoreID +'
		ALTER COLUMN CurrentSalesRepUserID varchar(32) '
		

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
--  ALTER TABLE _iDeals (GlobalCustomerID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iDeals (GlobalCustomerID)';

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

		ALTER TABLE ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
		ALTER COLUMN GlobalCustomerID varchar(32) '
		

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
--  ALTER TABLE _iDeals (CoBuyerGlobalCustomerID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iDeals (CoBuyerGlobalCustomerID)';

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

		ALTER TABLE ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
		ALTER COLUMN CoBuyerGlobalCustomerID varchar(32) '
		

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
--  ALTER TABLE _iDeals (AutoLeadID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iDeals (AutoLeadID)';

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

		ALTER TABLE ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
		ALTER COLUMN AutoLeadID varchar(32) '
		

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
--  ALTER TABLE _iDeals (CreatedByUserID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iDeals (CreatedByUserID)';

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

		ALTER TABLE ' + @TableLocation + '_iDeals'+ @DynamicStoreID +'
		ALTER COLUMN CreatedByUserID varchar(32) '
		

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
--  ALTER TABLE _iNotes (AutoLeadID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iNotes (AutoLeadID)';

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

		ALTER TABLE ' + @TableLocation + '_iNotes'+ @DynamicStoreID +'
		ALTER COLUMN AutoLeadID varchar(32) '
		

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
--  ALTER TABLE _iNotes (CreatedByUserID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iNotes (CreatedByUserID)';

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

		ALTER TABLE ' + @TableLocation + '_iNotes'+ @DynamicStoreID +'
		ALTER COLUMN CreatedByUserID varchar(32) '
		

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
--  ALTER TABLE _iNotes (LeadMessageTypeName)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iNotes (LeadMessageTypeName)';

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

		ALTER TABLE ' + @TableLocation + '_iNotes'+ @DynamicStoreID +'
		ALTER COLUMN LeadMessageTypeName varchar(255) '
		

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
--  ALTER TABLE _iTrades (AutoLeadID)
------------------------------------
SET @ChildStageName  = 'ALTER TABLE _iTrades (AutoLeadID)';

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

		ALTER TABLE ' + @TableLocation + '_iTrades'+ @DynamicStoreID +'
		ALTER COLUMN AutoLeadID varchar(32) '
		

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
SET @ChildStageName  = 'StagingCleanup_Vin Successfully Completed';

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
		PRINT ''Vin Staging Data - Clean and Transform has completed successfully.  Please check ImportHistory and ImportError for details'''

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

	
