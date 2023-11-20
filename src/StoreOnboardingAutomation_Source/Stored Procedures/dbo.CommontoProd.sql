SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[CommontoProd]   
	@GUID UNIQUEIDENTIFIER

AS

/* 

NEW ONBOARDING PROCESS

4.  Load Common Tables to PROD 


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
		@ParentStage INT = (SELECT pkImportParentStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportParentStage] WHERE ParentStageName = 'CommontoProd'),
		@ChildStage INT,
		@ChildStageName NVARCHAR(100),
		@TableCount INT,
		@sqlcmd NVARCHAR(MAX),
		@LeadcrumbTable VARCHAR(100),
		@LeadcrumbTableStartCount INT,
		@CompletionCheck BIT;

		
------------------------------------
--  INSERT USER
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert User')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert User';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'User'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[User] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
	
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

				INSERT INTO [' + @leadcrumb + '].[dbo].[User] (	 	
					[fkStoreID],
					[Email],
					[Password],
					[FirstName],
					[LastName],
					[Title],
					[UserType],
					[DateCreated],
					[DateModified],
					[IsDeleted],
					[IsActive],	
					[SessionStoreID],
					[GUID],
					[ForwardEmailTo],
					[CellPhone],
					[Biography]	)
				SELECT
					'+ @StoreID +',
					'''',
					'''',
					FirstName,
					LastName,
					'''',
					1,
					CURRENT_TIMESTAMP, 
					CURRENT_TIMESTAMP, 
					0, 
					1, 
					'+ @StoreID +', 
					NEWID(), 
					'''', -- email
					'''', -- phone
					[CRMUserID]

				FROM [_Common].[dbo].[CommonUser]
				WHERE FKImportLogID = ' + @ImportID

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
	END

--------------------------------------
----  INSERT CUSTOMER
--------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert Customer')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert Customer';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'Customer'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[Customer] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
	
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

				INSERT INTO [' + @leadcrumb + '].[dbo].[Customer] (	 
					[PrimaryFirstName],
					[PrimaryLastName],
					[fkStoreID],
					[CoFirstName],
					[CoLastName],
					[Email],
					[CoEmail],
					[CellPhone],
					[PrimaryDOB],	
					[Address1],
					[Address2],
					[City],
					[State],
					[Zip],
					[Image],
					[DateCreated],
					[DateModified],
					[IsDeleted],
					[GUID],
					[DMSImportID],
					[CustomerType],
					[IsUnderReview],
					[CompanyName],
					[HomePhone],
					[WorkPhone],
					[DateLastVisit],
					[ProspectOdds],
					[ProspectTimeline],
					[ProspectStatus],
					[ProspectSource],
					[ProspectLostReason],
					[DateLastService],
					[fkUserIDSales1],
					[fkUserIDSales2],
					[fkUserIDService],
					[ExtendedID],
					[fkOrphanedUserID],
					[OrphanedDate],
					[IsPossibleMatch],
					[PrimaryMiddleName],
					[PersonalNotes],
					[fkUserIDBDC],
					[fkMergedToCustomerID],
					[CoMiddleName],
					[WorkPhoneExtension]
				)

				SELECT
					PrimaryFirstName,
					PrimaryLastName,
					'+ @StoreID +' AS fkStoreID,
					'''' AS CoFirstName,
					'''' AS CoLastName,
					CASE		
						WHEN sc.[Name] = ''Vin'' THEN cc.Email
						ELSE ''''
					END AS Email,
					'''' AS CoEmail,
					'''' as CellPhone,
					PrimaryDOB,	
					Address1,
					Address2,
					City,
					State,
					Zip,
					NULL AS [Image],
					CustomerDateCreated ,
					CURRENT_TIMESTAMP AS DateModified,
					0 AS IsDeleted,
					NEWID() as [GUID],
					0 AS DMSImportID,
					CustomerType,
					0 AS IsUnderReview,
					CompanyName,
					'''' as HomePhone,
					'''' as WorkPhone,
					NULL AS DateLastVisit,
					0 as ProspectOdds,
					'''' AS ProspectTimeline,
					0 as ProspectStatus,
					CASE
						WHEN sc.[Name] = ''Elead'' THEN ''ELEADS CRM IMPORT''
						WHEN sc.[Name] = ''Vin'' THEN ''VIN CRM IMPORT''
						WHEN sc.[Name] = ''DealerPeak'' THEN ''DEALERPEAK''
						WHEN sc.[Name] = ''DealerSocket'' THEN ''DEALERSOCKET''
						WHEN sc.[Name] = ''XRM'' THEN ''XRM''
						WHEN sc.[Name] = ''AutoRaptor'' THEN ''AUTORAPTOR''
						WHEN sc.[Name] = ''CRMSuite'' THEN ''CRMSUITE''
						WHEN sc.[Name] = ''Reynolds'' THEN ''REYNOLDS''
						WHEN sc.[Name] = ''Momentum''  THEN ''MOMENTUM''
						WHEN sc.[Name] = ''AutoBase''  THEN ''AUTOBASE''
						WHEN sc.[Name] = ''HigherGear''  THEN ''HIGHERGEAR''
						WHEN sc.[Name] = ''CDK''  THEN ''CDK''
						WHEN sc.[Name] = ''Vision''  THEN ''VISION''
						WHEN sc.[Name] = ''Dominion''  THEN ''DOMINION''
						WHEN sc.[Name] = ''iMagic''  THEN ''IMAGIC''
						WHEN sc.[Name] = ''Tekkion''  THEN ''TEKKION''
						WHEN sc.[Name] = ''ProMax''  THEN ''PROMAX''
						WHEN sc.[Name] = ''Custom - D2C Media''  THEN ''CUSTOM''
						ELSE ''''
					END AS ProspectSource,
					'''' AS ProspectLostReason,
					''1-1-1900'' AS DateLastService,
					0 AS fkUserIDSales1,
					0 AS fkUserIDSales2,
					0 AS fkUserIDService,
					'''' AS ExtendedID,
					0 AS fkOrphanedUserID,
					NULL AS OrphanedDate,
					0 AS IsPossibleMatch,
					PrimaryMiddleName,
					CRMCustomerID AS PersonalNotes,
					0 AS fkUserIDBDC,
					0 AS fkMergedToCustomerID,
					'''' AS CoMiddleName,
					'''' AS WorkPhoneExtension

				FROM [_Common].[dbo].[CommonCustomer] cc
				INNER JOIN #ImportLog il
					ON cc.FKImportLogID = il.PKImportID
				INNER JOIN #CrmImportSourceConfig sc
					ON il.FKCRMID = sc.CrmImportSourceConfigId
				WHERE FKImportLogID = ' + @ImportID

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
	END

--------------------------------------
----  INSERT CUSTOMERCONTACT - EMAIL
--------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert CustomerContact - Email')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CustomerContact - Email';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'CustomerContact'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[CustomerContact] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[CustomerContact] (	 
					[fkCustomerID],
					[ContactLabelType],
					[CommunicationType],
					[Value],
					[IsPreferred],
					[Description],
					[IsOnDNC],
					[IsActive],
					[IsBad],
					[DateCreated],
					[DateModified],
					[IsDeleted],
					[FullCDYNEInfo],
					[GUID],
					[FullTwilioInfo]
				)

				SELECT
					c.pkCustomerID,
					1, -- personal
					2, -- email
					[Value],
					1, -- preferred,
					CASE
						WHEN sc.[Name] = ''Elead'' THEN ''Imported from eLeads''
						WHEN sc.[Name] = ''Vin'' THEN ''Imported from Vin''
						WHEN sc.[Name] = ''DealerPeak'' THEN ''DEALERPEAK''
						WHEN sc.[Name] = ''DealerSocket'' THEN ''DEALERSOCKET''
						WHEN sc.[Name] = ''XRM'' THEN ''XRM''
						WHEN sc.[Name] = ''AutoRaptor'' THEN ''AUTORAPTOR''
						WHEN sc.[Name] = ''CRMSuite'' THEN ''CRMSUITE''
						WHEN sc.[Name] = ''Reynolds'' THEN ''REYNOLDS''
						WHEN sc.[Name] = ''Momentum''  THEN ''MOMENTUM''
						WHEN sc.[Name] = ''AutoBase''  THEN ''AUTOBASE''
						WHEN sc.[Name] = ''HigherGear''  THEN ''HIGHERGEAR''
						WHEN sc.[Name] = ''CDK''  THEN ''CDK''
						WHEN sc.[Name] = ''Vision''  THEN ''VISION''
						WHEN sc.[Name] = ''Dominion''  THEN ''DOMINION''
						WHEN sc.[Name] = ''iMagic''  THEN ''IMAGIC''
						WHEN sc.[Name] = ''Tekkion''  THEN ''TEKKION''
						WHEN sc.[Name] = ''ProMax''  THEN ''PROMAX''
						WHEN sc.[Name] = ''Custom - D2C Media''  THEN ''CUSTOM''
						ELSE ''''
					END,
					0,
					0, 
					0,
					CustomerContactDateCreated,
					CURRENT_TIMESTAMP,
					0,
					'''',
					NEWID(),
					''''
	
				FROM [_Common].[dbo].[CommonCustomerContact] cc
				INNER JOIN #ImportLog il
					ON cc.FKImportLogID = il.PKImportID
				INNER JOIN #CrmImportSourceConfig sc
					ON il.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = cc.fkCustomerID 
					AND ISNULL(c.PersonalNotes,'''') <> '''' 
					AND c.fkStoreID = '+ @StoreID +'
				WHERE cc.CommunicationType = 2 AND
				FKImportLogID = ' + @ImportID

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
	END

--------------------------------------
---- INSERT CUSTOMERCONTACT - PHONE
--------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert CustomerContact - Phone')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CustomerContact - Phone';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'CustomerContact'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[CustomerContact] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[CustomerContact] (	 
					[fkCustomerID],
					[ContactLabelType],
					[CommunicationType],
					[Value],
					[IsPreferred],
					[Description],
					[IsOnDNC],
					[IsActive],
					[IsBad],
					[DateCreated],
					[DateModified],
					[IsDeleted],
					[FullCDYNEInfo],
					[GUID],
					[FullTwilioInfo]
				)

				SELECT
					c.pkCustomerID,
					cc.ContactLabelType, -- 1 Personal, 2 Cellular, 3 Work Phone
					1, -- phone
					[Value],
					1, -- preferred,
					CASE
						WHEN sc.[Name] = ''Elead'' THEN ''Imported from eLeads''
						WHEN sc.[Name] = ''Vin'' THEN ''Imported from Vin''
						WHEN sc.[Name] = ''DealerPeak'' THEN ''DEALERPEAK''
						WHEN sc.[Name] = ''DealerSocket'' THEN ''DEALERSOCKET''
						WHEN sc.[Name] = ''XRM'' THEN ''XRM''
						WHEN sc.[Name] = ''AutoRaptor'' THEN ''AUTORAPTOR''
						WHEN sc.[Name] = ''CRMSuite'' THEN ''CRMSUITE''
						WHEN sc.[Name] = ''Reynolds'' THEN ''REYNOLDS''
						WHEN sc.[Name] = ''Momentum''  THEN ''MOMENTUM''
						WHEN sc.[Name] = ''AutoBase''  THEN ''AUTOBASE''
						WHEN sc.[Name] = ''HigherGear''  THEN ''HIGHERGEAR''
						WHEN sc.[Name] = ''CDK''  THEN ''CDK''
						WHEN sc.[Name] = ''Vision''  THEN ''VISION''
						WHEN sc.[Name] = ''Dominion''  THEN ''DOMINION''
						WHEN sc.[Name] = ''iMagic''  THEN ''IMAGIC''
						WHEN sc.[Name] = ''Tekkion''  THEN ''TEKKION''
						WHEN sc.[Name] = ''ProMax''  THEN ''PROMAX''
						WHEN sc.[Name] = ''Custom - D2C Media''  THEN ''CUSTOM''
						ELSE ''''
					END,
					0,
					0, 
					0,
					CustomerContactDateCreated,
					CURRENT_TIMESTAMP,
					0,
					'''',
					NEWID(),
					''''

				FROM [_Common].[dbo].[CommonCustomerContact] cc
				INNER JOIN #ImportLog il
					ON cc.FKImportLogID = il.PKImportID
				INNER JOIN #CrmImportSourceConfig sc
					ON il.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = cc.fkCustomerID 
					AND ISNULL(c.PersonalNotes,'''') <> '''' 
					AND c.fkStoreID = '+ @StoreID +'
				WHERE cc.CommunicationType = 1 AND
				FKImportLogID = ' + @ImportID

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
	END

--------------------------------------
---- INSERT DEAL
--------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert Deal')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert Deal';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'Deal'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[Deal] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[Deal] (	 
					[fkStoreID],
					[fkCustomerID],
					[fkUserIDSales1],
					[fkUserIDSales2],
					[Odds],
					[SourceType],
					[SourceDescription],
					[LostReason],
					[LostReasonDescription],
					[PreferredContactType],
					[TimeFrame],
					[DateTimeFrame],
					[DateCreated],
					[DateModified],
					[IsDeleted],
					[fkDealStatusID],
					[fkILMUserID],
					[DateInStore],
					[fkDuplicateDealID],
					[ADFXML],
					[DateAssigned],
					[DateLimbo],
					[DateDuplicateConfirmed],
					[SourceSubType]
				)

				SELECT
					'+ @StoreID +',
					c.pkCustomerID AS fkCustomerID,
					ISNULL(u1.pkUserID,'''') AS fkUserIDSales1,
					ISNULL(u2.pkUserID,'''') AS fkUserIDSales2,
					0 AS Odds,
					cd.SourceType,
					cd.SourceDescription,
					0 AS LostReason,
					'''' AS LostReasonDescription,
					0 AS PreferredContactType,
					cd.CRMDealID, --TimeFrame
					NULL AS DateTimeFrame,
					cd.DealDateCreated,
					CURRENT_TIMESTAMP AS DateModified,
					0 AS IsDeleted,
					0 AS fkDealStatusID,
					ISNULL(ub.pkUserID,'''')  AS fkILMUserID,
					NULL AS DateInStore,
					0 AS fkDuplicateDealID,
					CASE
						WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
						WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
						WHEN sc.[Name] = ''DealerPeak'' THEN ''DEALERPEAK''
						WHEN sc.[Name] = ''DealerSocket'' THEN ''DEALERSOCKET''
						WHEN sc.[Name] = ''XRM'' THEN ''XRM''
						WHEN sc.[Name] = ''AutoRaptor'' THEN ''AUTORAPTOR''
						WHEN sc.[Name] = ''CRMSuite'' THEN ''CRMSUITE''
						WHEN sc.[Name] = ''Reynolds'' THEN ''REYNOLDS''
						WHEN sc.[Name] = ''Momentum''  THEN ''MOMENTUM''
						WHEN sc.[Name] = ''AutoBase''  THEN ''AUTOBASE''
						WHEN sc.[Name] = ''HigherGear''  THEN ''HIGHERGEAR''
						WHEN sc.[Name] = ''CDK''  THEN ''CDK''
						WHEN sc.[Name] = ''Vision''  THEN ''VISION''
						WHEN sc.[Name] = ''Dominion''  THEN ''DOMINION''
						WHEN sc.[Name] = ''iMagic''  THEN ''IMAGIC''
						WHEN sc.[Name] = ''Tekkion''  THEN ''TEKKION''
						WHEN sc.[Name] = ''ProMax''  THEN ''PROMAX''
						WHEN sc.[Name] = ''Custom - D2C Media''  THEN ''CUSTOM''
						ELSE ''''		
					END AS ADFXML,
					NULL AS DateAssigned,
					NULL AS DateLimbo,
					NULL AS DateDuplicateConfirmed,
					0    AS SourceSubType -- HACK TO MATCH DEALS TO Temp_notes

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog il
					ON cd.FKImportLogID = il.PKImportID
				INNER JOIN #CrmImportSourceConfig sc
					ON il.FKCRMID = sc.CrmImportSourceConfigId
				LEFT JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = cd.fkCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''	
					AND c.fkStoreID = '+ @StoreID +'
				LEFT JOIN [' + @leadcrumb + '].[dbo].[User] u1
					ON u1.[Biography] = cd.fkUserIDSales1
					AND ISNULL(u1.[Biography],'''') <> ''''
					AND u1.fkStoreID = '+ @StoreID +'
				LEFT JOIN [' + @leadcrumb + '].[dbo].[User] u2
					ON u2.[Biography] = cd.fkUserIDSales2
					AND ISNULL(u2.[Biography],'''') <> ''''
					AND u2.fkStoreID = '+ @StoreID +'
				LEFT JOIN [' + @leadcrumb + '].[dbo].[User] ub
					ON ub.[Biography] = cd.fkUserIDBDC
					AND ISNULL(ub.[Biography],'''') <> ''''
					AND ub.fkStoreID = '+ @StoreID +'
				WHERE FKImportLogID = ' + @ImportID

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
	END

IF @ManualDeliver = 1
	BEGIN
		-------------------------------------------------
		---- INSERT DEALFLAG - MANUAL - SOLD/DELIVERED
		-------------------------------------------------
		SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Manual - Sold/Delivered')
					
		IF @CompletionCheck = 0
			BEGIN
				SET @ChildStageName = 'Insert DealFlag - Manual - Sold/Delivered';

				--Pull Child Stage ID
				SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

				--Set Leadcrumb table name and count
				SET @LeadcrumbTable = 'DealFlag'
	
				SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
				EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

						INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	
						[fkDealID], 
						[DealFlagType], 
						[DateCreated], 
						[DateModified], 
						[IsDeleted], 
						[fkStoreID]
						)
						SELECT 
						pkDealID,
						8,
						cd.SoldDate,
						CURRENT_TIMESTAMP,
						0,
						'+ @StoreID +'

						FROM [_Common].[dbo].[CommonDeal] cd
						INNER JOIN #ImportLog i
							ON i.PKImportId = cd.FKImportLogID
						INNER JOIN #CrmImportSourceConfig sc
							ON i.FKCRMID = sc.CrmImportSourceConfigId
						INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK) 
							ON d.TimeFrame = cd.[CRMDealID] 
							AND d.ADFXML = CASE 
												WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''						
												ELSE ''''
											END
							AND d.fkStoreID = '+ @StoreID +'  -- Little hack joiner
						WHERE cd.Delivered = ''1''

						UNION

						SELECT 
						pkDealID,
						9,
						cd.SoldDate,
						CURRENT_TIMESTAMP,
						0,
						'+ @StoreID +'

						FROM [_Common].[dbo].[CommonDeal] cd
						INNER JOIN #ImportLog i
							ON i.PKImportId = cd.FKImportLogID
						INNER JOIN #CrmImportSourceConfig sc
							ON i.FKCRMID = sc.CrmImportSourceConfigId
						INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK) 
							ON d.TimeFrame = cd.[CRMDealID] 
							AND d.ADFXML = CASE 
												WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''						
												ELSE ''''
											END
							AND d.fkStoreID = '+ @StoreID +'  -- Little hack joiner
						WHERE cd.Delivered = ''1'''

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
			END

	END

--------------------------------------
----  INSERT DEALFLAG - SOLD
--------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Sold')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealFlag - Sold';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealFlag'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				--Add DealFlag 8 (Sold)
				INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	 
					[fkDealID],
					[DealFlagType], 
					[DateCreated], 
					[DateModified], 
					[IsDeleted],
					[fkStoreID] 
				)

				SELECT
					d.pkDealID, 
					8,
					CURRENT_TIMESTAMP, 
					CURRENT_TIMESTAMP,
					0, 
					'+ @StoreID +'

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog i
					ON i.PKImportId = cd.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK)
					ON d.TimeFrame = cd.[CRMDealID] 
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				WHERE NOT EXISTS (SELECT pkDealFlagID FROM [' + @leadcrumb + '].[dbo].DealFlag (NOLOCK) WHERE fkDealID = d.pkDealID AND DealFlagType IN (8))
				AND cd.SoldFlag = ''1'''

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
	END

-------------------------------------------
----  INSERT DEALFLAG - SOLD NOT DELIVERED
-------------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Sold Not Delivered')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealFlag - Sold Not Delivered';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealFlag'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				--Add DealFlag 11 (SoldNotDelivered)
				INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	 
					[fkDealID],
					[DealFlagType], 
					[DateCreated], 
					[DateModified], 
					[IsDeleted],
					[fkStoreID] 
				)

				SELECT
					d.pkDealID, 
					11,
					CURRENT_TIMESTAMP, 
					CURRENT_TIMESTAMP,
					0, 
					'+ @StoreID +'

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog i
					ON i.PKImportId = cd.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK) 
					ON d.TimeFrame = cd.[CRMDealID] 
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				WHERE NOT EXISTS (SELECT pkDealFlagID FROM [' + @leadcrumb + '].[dbo].DealFlag (NOLOCK) WHERE fkDealID = d.pkDealID AND DealFlagType IN (11))
				AND cd.SoldNotDeliveredFlag = ''1'''

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
	END

-------------------------------------------
---- INSERT DEALFLAG - ORDERED 
-------------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Ordered')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealFlag - Ordered';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealFlag'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				--Add DealFlag 3 for everything On Order (Ordered)
				INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	 
					[fkDealID],
					[DealFlagType], 
					[DateCreated], 
					[DateModified], 
					[IsDeleted],
					[fkStoreID] 
				)

				SELECT
					d.pkDealID, 
					3,
					CURRENT_TIMESTAMP, 
					CURRENT_TIMESTAMP,
					0, 
					'+ @StoreID +'

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog i
					ON i.PKImportId = cd.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK) 
					ON d.TimeFrame = cd.[CRMDealID] 
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				WHERE NOT EXISTS (SELECT pkDealFlagID FROM [' + @leadcrumb + '].[dbo].DealFlag (NOLOCK) WHERE fkDealID = d.pkDealID AND DealFlagType IN (3))
				AND cd.OrderedFlag = ''1'''

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
	END

-------------------------------------------
---- INSERT DEALFLAG - PENDING
-------------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Pending')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealFlag - Pending';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealFlag'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				--Add DealFlag 2 for everything Pre Orders (Pending)
				INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	 
					[fkDealID],
					[DealFlagType], 
					[DateCreated], 
					[DateModified], 
					[IsDeleted],
					[fkStoreID] 
				)

				SELECT
					d.pkDealID, 
					2,
					CURRENT_TIMESTAMP, 
					CURRENT_TIMESTAMP,
					0, 
					'+ @StoreID +'

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog i
					ON i.PKImportId = cd.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK) 
					ON d.TimeFrame = cd.[CRMDealID] 
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				WHERE NOT EXISTS (SELECT pkDealFlagID FROM [' + @leadcrumb + '].[dbo].DealFlag (NOLOCK) WHERE fkDealID = d.pkDealID AND DealFlagType IN (2))
				AND cd.PendingFlag = ''1'''


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
	END

-------------------------------------------
---- INSERT DEALFLAG - DEAD (VIN)
-------------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Dead (Vin)')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealFlag - Dead (Vin)';
		
			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealFlag'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				IF OBJECT_ID(''tempdb..##VinDeadDeals'') IS NOT NULL
				DROP TABLE ##VinDeadDeals

				CREATE TABLE ##VinDeadDeals (pkDealID INT) 

				--Add DealFlag 5 (Dead)
				INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	 
					[fkDealID],
					[DealFlagType], 
					[DateCreated], 
					[DateModified], 
					[IsDeleted],
					[fkStoreID] 
				)
				OUTPUT Inserted.fkDealID INTO ##VinDeadDeals
				SELECT
					d.pkDealID, 
					5,
					CURRENT_TIMESTAMP, 
					CURRENT_TIMESTAMP,
					0, 
					'+ @StoreID +'

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog i
					ON i.PKImportId = cd.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK) 
					ON d.TimeFrame = cd.[CRMDealID] 
					AND d.ADFXML = CASE 						
										WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				WHERE NOT EXISTS (SELECT pkDealFlagID FROM [' + @leadcrumb + '].[dbo].DealFlag (NOLOCK) WHERE fkDealID = d.pkDealID AND DealFlagType IN (5,9))
				AND cd.ServiceFlag = ''1'''

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
	END

-------------------------------------------
---- INSERT DEALLOG - DEAD DEALS (VIN)
-------------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealLog - Dead Deals (Vin)')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealLog - Dead Deals (Vin)';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealLog'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealLog] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[DealLog] (
					fkDealID, 
					fkUserID, 
					fkCustomerVehicleID, 
					fkStoreID, 
					fkCustomerID, 
					fkDealStatusID, 
					fkCrumbID, 
					fkTaskID, 
					fkAppointmentID, 
					DealLogType, 
					[Message], 
					DateCreated, 
					DateModified, 
					IsDeleted, 
					DealFlagType, 
					fkReferenceID, 
					ResultType)--, ResponseTimeSeconds, Tally
				SELECT 
					d.pkDealID, 
					d.fkUserIDSales1, 
					0, 
					d.fkstoreid, 
					d.fkcustomerid, 
					0, 
					0, 
					0, 
					0, 
					108, 
					''Import script killed the deal (VinSolutions Create CRM Items)'', 
					DATEADD(HOUR, 12, DateCreated), 
					DateCreated, 
					0, 
					0, 
					0, 
					0
				FROM [' + @leadcrumb + '].[dbo].Deal d
				INNER JOIN ##VinDeadDeals dd ON d.pkDealID = dd.pkDealID'


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
	END

-----------------------------------------------
---- INSERT DEALFLAG - DEAD (BAD OR LOST) (VIN)
-----------------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Dead (Bad or Lost) (Vin)')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealFlag - Dead (Bad or Lost) (Vin)';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealFlag'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				--Add DealFlag 5 (Dead)
				INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	 
					[fkDealID],
					[DealFlagType], 
					[DateCreated], 
					[DateModified], 
					[IsDeleted],
					[fkStoreID] 
				)
				SELECT
					d.pkDealID, 
					5,
					cd.DealDateCreated, 
					CURRENT_TIMESTAMP,
					0, 
					'+ @StoreID +'

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog i
					ON i.PKImportId = cd.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK) 
					ON d.TimeFrame = cd.[CRMDealID] 
					AND d.ADFXML = CASE 						
										WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				WHERE NOT EXISTS (SELECT pkDealFlagID FROM [' + @leadcrumb + '].[dbo].DealFlag (NOLOCK) WHERE fkDealID = d.pkDealID AND DealFlagType IN (5))
				AND cd.DeadFlag = ''1'''

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
	END

------------------------------------
--  TIE USERS TO CUSTOMER
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Tie Users to Customer')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Tie Users to Customer';

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

				IF OBJECT_ID(''tempdb..#CommonCustomerTemp'') IS NOT NULL
				DROP TABLE #CommonCustomerTemp
				CREATE TABLE #CommonCustomerTemp
				([CRMCustomerID] VARCHAR(50),[FKImportLogID] INT)
				CREATE CLUSTERED INDEX [ix_commoncustomer_CRMID_ImportLogID] ON #CommonCustomerTemp ([CRMCustomerID],[FKImportLogID]);
				INSERT INTO #CommonCustomerTemp 
				SELECT DISTINCT [CRMCustomerID],[FKImportLogID] FROM [_Common].[dbo].[CommonCustomer]
				WHERE FKImportLogID = '+ @ImportID +'


				UPDATE [' + @leadcrumb + '].[dbo].[Customer]
				SET fkUserIDSales1 = ISNULL(d.fkUserIDSales1, 0),
					fkUserIDSales2 = ISNULL(d.fkUserIDSales2, 0),
					fkUserIDBDC = ISNULL(d.fkILMUserID, 0)
				FROM [' + @leadcrumb + '].[dbo].Deal d
				INNER JOIN [' + @leadcrumb + '].[dbo].Customer c 
					ON c.pkCustomerID = d.fkCustomerID AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN #CommonCustomerTemp cc
					ON c.PersonalNotes = cc.CRMCustomerID	
				INNER JOIN #ImportLog i
					ON i.PKImportId = cc.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				WHERE d.fkStoreID = '+ @StoreID +'
					AND d.fkCustomerID = c.pkCustomerID
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
										ELSE ''''
									END'


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
	END

-------------------------------------------
----  INSERT CUSTOMERVEHICLE - SOLD VEHICLE
-------------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert CustomerVehicle - Sold Vehicle')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CustomerVehicle - Sold Vehicle';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'CustomerVehicle'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[CustomerVehicle] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[CustomerVehicle] (	 
					[fkCustomerID],
					[fkStoreID],
					[fkUserID],
					[DateCreated],
					[fkDealID],
					[NewUsedType],
					[InterestType],
					[Year],
					[Make],
					[Model],
					[OdometerStatus],
					[VIN],
					[DateModified],
					[ExteriorColor],
					[InteriorColor],
					[StockNumber],
					[Trim],
					[Comments]
				)

				SELECT 
					d.fkCustomerID,
					d.fkStoreID,
					d.fkUserIDSales1 AS fkUserID,
					d.DateCreated,
					d.pkDealID,
					cv.NewUsedType,
					1 AS InterestType,
					cv.[Year],
					cv.Make,
					cv.[Model],
					cv.OdometerStatus,
					cv.VIN,
					CURRENT_TIMESTAMP AS DateModified,
					cv.ExteriorColor,
					cv.InteriorColor,
					cv.StockNumber, 
					cv.[Trim],
					CASE 
						WHEN sc.[Name] = ''Elead'' THEN ''ELEADS PROSPECT IMPORT''
						WHEN sc.[Name] = ''Vin'' THEN ''VIN PROSPECT IMPORT''
						ELSE ''''
					END AS Comments
				FROM [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK)
				INNER JOIN [_Common].[dbo].[CommonCustomerVehicle] cv
					ON d.TimeFrame = cv.FKDealID
				INNER JOIN #ImportLog i
					ON i.PKImportId = cv.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				WHERE FKImportLogID = ' + @ImportID + '
				AND ISNULL(cv.InterestType,'''') = 1'


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
	END

------------------------------------
--  INSERT CUSTOMERVEHICLE - TRADE
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert CustomerVehicle - Trade')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CustomerVehicle - Trade';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'CustomerVehicle'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[CustomerVehicle] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[CustomerVehicle] (	 
					fkCustomerID,
					fkStoreID,
					fkUserID,
					DateCreated,
					fkDealID,
					NewUsedType,
					InterestType,
					[Year],
					Make,
					[Model],
					OdometerStatus,
					VIN,
					DateModified,
					ExteriorColor,
					InteriorColor,
					StockNumber,
					Trim,
					Comments
				)
				SELECT 
					d.fkCustomerID,
					d.fkStoreID,
					d.fkUserIDSales1 AS fkUserID,
					d.DateCreated,
					d.pkDealID,
					cv.NewUsedType,
					cv.InterestType, -- trade
					cv.[Year],
					cv.Make,
					cv.[Model],
					cv.OdometerStatus,
					cv.VIN,
					CURRENT_TIMESTAMP AS DateModified,	
					cv.ExteriorColor,
					cv.InteriorColor,
					cv.StockNumber, 
					cv.[Trim],
					CASE 
						WHEN sc.[Name] = ''Elead'' THEN ''ELEADS PROSPECT IMPORT''
						WHEN sc.[Name] = ''Vin'' THEN ''VIN PROSPECT IMPORT''
						ELSE ''''
					END AS Comments
				FROM [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK)
				INNER JOIN [_Common].[dbo].[CommonCustomerVehicle] cv
					ON d.TimeFrame = cv.FKDealID
				INNER JOIN #ImportLog i
					ON i.PKImportId = cv.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				WHERE FKImportLogID = ' + @ImportID + '
				AND ISNULL(cv.InterestType,'''') = 4'

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
	END

-----------------------------------------
----  SET STOCKNUMBER ON CUSTOMERVEHICLE
-----------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Set StockNumber on CustomerVehicle')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Set StockNumber on CustomerVehicle';

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
				UPDATE [' + @leadcrumb + '].[dbo].[CustomerVehicle]
				SET StockNumber = v.StockNumber
				FROM [' + @leadcrumb + '].[dbo].[CustomerVehicle] cv
				INNER JOIN [' + @leadcrumb + '].dbo.Deal d 
					ON d.pkDealID = cv.fkDealID AND d.fkStoreID = '+ @StoreID +'
				INNER JOIN [' + @leadcrumb + '].dbo.Vehicle v 
					ON v.VIN = cv.VIN AND v.fkStoreID = '+ @StoreID +''

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
	END

------------------------------------
--  UPDATE SALES ON CUSTOMER (VIN)
------------------------------------
--VIN Specific
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Update Sales on Customer (Vin)')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Update Sales on Customer (Vin)';

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
				-- Update salesperson of customer
				UPDATE c
				SET c.fkUserIDSales1 = u.pkUserID
				FROM [_Common].[dbo].[CommonCustomer] cc
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c 
					ON c.PersonalNotes = cc.CRMCustomerID AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN [' + @leadcrumb + '].[dbo].[User] u
					ON u.Biography = cc.Sales1UserID AND u.fkStoreID = '+ @StoreID +'
				WHERE ISNUMERIC(Sales1UserID) = 1 AND Sales1UserID > 0
				AND c.ProspectSource = ''VIN CRM IMPORT'''

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
	END

------------------------------------
--  UPDATE BDC ON CUSTOMER (VIN)
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Update BDC on Customer (Vin)')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Update BDC on Customer (Vin)';

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
				-- Update bdc user ON customer
				UPDATE c
				SET c.fkUserIDBDC = u.pkUserID
				FROM [_Common].[dbo].[CommonCustomer] cc
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c 
					ON c.PersonalNotes = cc.CRMCustomerID AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN [' + @leadcrumb + '].[dbo].[User] u
					ON u.Biography = cc.BDCUserID AND u.fkStoreID = '+ @StoreID +'
				WHERE ISNUMERIC(BDCUserID) = 1 AND BDCUserID > 0
				AND c.ProspectSource = ''VIN CRM IMPORT'''

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
	END

------------------------------------
--  UPDATE BDC ON DEAL (VIN)
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Update BDC on Deal (Vin)')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Update BDC on Deal (Vin)';

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
				-- Update bdc user ON deal
				UPDATE d
				SET fkILMUserID = c.fkUserIDBDC
				FROM [_Common].[dbo].[CommonCustomer] cc
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c 
					ON c.PersonalNotes = cc.CRMCustomerID AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d (NOLOCK)
					ON d.fkCustomerID = c.pkCustomerID AND d.fkILMUserID = 0 AND d.IsDeleted = 0 AND d.fkDuplicateDealID = 0
				WHERE c.fkUserIDBDC > 0
				AND c.IsDeleted = 0
				AND c.ProspectSource = ''VIN CRM IMPORT'''


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
	END

--------------------------------------
---- INSERT TASK - CALLS
--------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert Task - Calls')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert Task - Calls';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'Task'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[Task] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[Task] (	 
					[fkUserID],
					[fkCreatedByUserID],
					[fkCompletedByUserID],
					[fkStoreID],
					[fkCustomerID],
					[Subject],
					[Description],
					[Resolution],
					[DateStart],
					[DateDue],
					[CompletionPercentage],
					[Type],
					[DateModified],
					[DateCreated],
					[DateCompleted],
					[IsDeleted],
					[fkDealID],
					[ActionType],
					[ResultType],
					[DepartmentType]
				)


				SELECT 
					ISNULL(u1.pkUserID,0), --UserID
					ISNULL(u2.pkUserID,0), --CreatedByUserID
					ISNULL(u3.pkUserID,0), --CompletedByUserID
					'+ @StoreID +',
					c.pkCustomerID as CustomerID,
					[Subject],
					[Description],
					Resolution,
					ct.DateStart,
					DateDue,
					100,
					301,
					''1-1-1930'',
					TaskDateCreated,
					TaskDateCompleted,
					0,
					ISNULL(d.pkDealID, 0) as DealID,
					2, --ActionType
					ResultType,
					0 as DepartmentType
				FROM [_Common].[dbo].[CommonTask] ct
				INNER JOIN #ImportLog i
					ON i.PKImportId = ct.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = ct.fkCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d
					ON d.TimeFrame = ct.[FKDealID] 
					AND ISNULL(ct.[FKDealID],'''') <> ''''
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				LEFT JOIN [' + @leadcrumb + '].[dbo].[User] u1
					ON u1.[Biography] = ct.fkUserID
					AND ISNULL(u1.[Biography],'''') <> ''''
					AND u1.fkStoreID = '+ @StoreID +'
				LEFT JOIN [' + @leadcrumb + '].[dbo].[User] u2
					ON u2.[Biography] = ct.fkCreatedByUserID
					AND ISNULL(u2.[Biography],'''') <> ''''
					AND u2.fkStoreID = '+ @StoreID +'
				LEFT JOIN [' + @leadcrumb + '].[dbo].[User] u3
					ON u3.[Biography] = ct.fkCompletedByUserID
					AND ISNULL(u3.[Biography],'''') <> ''''
					AND u3.fkStoreID = '+ @StoreID +'

				WHERE FKImportLogID = ' + @ImportID


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
	END

------------------------------------
--  INSERT CUSTOMERLOG - NOTES
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert CustomerLog - Notes')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert CustomerLog - Notes';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'CustomerLog'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[CustomerLog] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[CustomerLog] (	 
					[fkCustomerID],
					[fkStoreID],
					[ContactType],
					[Notes],
					[IsContacted],
					[DateCreated],
					[DateModified],
					[fkUserID],
					[fkDealID],
					[IsDeleted]
				)

				SELECT
					c.pkCustomerID,
					'+ @StoreID +',
					8,
					[Notes],
					0,
					CustomerLogDateCreated,
					cl.[DateModified],
					u1.[pkUserID],
					d.[pkDealID],
					0

				FROM [_Common].[dbo].[CommonCustomerLog] cl
				INNER JOIN #ImportLog i
					ON i.PKImportId = cl.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = cl.FKCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN  [' + @leadcrumb + '].[dbo].[Deal] d
					ON d.TimeFrame = cl.[FKDealID] 
					AND ISNULL(cl.[FKDealID],'''') <> ''''
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				INNER JOIN  [' + @leadcrumb + '].[dbo].[User] u1
					ON u1.[Biography] = cl.FKUserIDSales1
					AND ISNULL(u1.[Biography],'''') <> ''''
					AND u1.fkStoreID = '+ @StoreID +'
				WHERE FKImportLogID = ' + @ImportID

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
	END

------------------------------------
--  INSERT DEALLOG - SHOWROOM VISIT
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealLog - Showroom Visit')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealLog - Showroom Visit';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealLog'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealLog] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				IF OBJECT_ID(''tempdb..#Customer'') IS NOT NULL
					DROP TABLE #Customer
				SELECT * INTO #Customer FROM [' + @leadcrumb + '].[dbo].[Customer] WHERE fkStoreID = '+ @StoreID +'		
				CREATE NONCLUSTERED INDEX [ix_customer_personalnotes] ON #Customer ([fkStoreID]) INCLUDE ([pkCustomerID],[PersonalNotes])


				IF OBJECT_ID(''tempdb..#Deal'') IS NOT NULL
					DROP TABLE #Deal
				SELECT * INTO #Deal from [' + @leadcrumb + '].[dbo].[Deal] WHERE fkStoreID = '+ @StoreID +'
				CREATE NONCLUSTERED INDEX [ix_deal_pkdealid] ON #Deal ([fkStoreID]) INCLUDE ([pkDealID],[TimeFrame],[ADFXML])
				CREATE NONCLUSTERED INDEX [ix_deal_pkdealid_timeframe] ON #Deal ([fkStoreID],[TimeFrame]) INCLUDE ([pkDealID],[ADFXML])


				INSERT INTO [' + @leadcrumb + '].[dbo].[DealLog] (	 
					[fkDealID],
					[fkUserID],
					[fkCustomerVehicleID],
					[fkStoreID],
					[fkCustomerID],
					[fkDealStatusID],
					[fkCrumbID],
					[fkTaskID],
					[fkAppointmentID],
					[DealLogType],
					[Message],
					[DateCreated],
					[DateModified],
					[IsDeleted],
					[DealFlagType],
					[fkReferenceID],
					[ResultType]
				)

				SELECT
					d.[pkDealID],
					ISNULL(u1.[pkUserID], 0),
					0,	
					'+ @StoreID +',
					c.[pkCustomerID],
					0,
					0,
					0,
					0,
					dl.[DealLogType],
					''Import Visit'',
					dl.[DealLogDateCreated],
					dl.[DateModified],
					0,
					0,
					0,
					0

				FROM [_Common].[dbo].[CommonDealLog] dl
				INNER JOIN #ImportLog i
					ON i.PKImportId = dl.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN #Customer c
					ON c.PersonalNotes = dl.FKCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN #Deal d
					ON d.TimeFrame = dl.[FKDealID] 
					AND ISNULL(dl.[FKDealID],'''') <> ''''
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				INNER JOIN  [' + @leadcrumb + '].[dbo].[User] u1
					ON u1.[Biography] = dl.FKUserIDSales1
					AND ISNULL(u1.[Biography],'''') <> ''''
					AND u1.fkStoreID = '+ @StoreID +'
				WHERE FKImportLogID = ' + @ImportID


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
	END

------------------------------------
--  INSERT OPTOUT - DNC
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert OptOut - DNC')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert OptOut - DNC';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'OptOut'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[OptOut] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[OptOut] (	 
					[fkStoreID],
					[fkCustomerID],
					[fkCrumbID],
					[fkCrumbTypeID],
					[fkAutoCrumbID],
					[DateCreated],
					[Email],
					[OptOutType]
				)

				SELECT  
					'+ @StoreID +' AS fkStoreID ,
					c.pkCustomerID AS fkCustomerID ,
					0 AS fkCrumbID ,
					0 AS fkCrumbTypeID ,
					0 AS fkAutoCrumbID ,
					GETDATE() AS DateCreated ,
					ISNULL(cc.Email, '''') AS Email ,
					2 AS OptOutType --Phone 
				FROM [_Common].[dbo].[CommonCustomer] cc
				INNER JOIN #ImportLog i
					ON i.PKImportId = cc.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = cc.CRMCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				WHERE cc.DoNotCall = 1
				AND FKImportLogID = ' + @ImportID

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
	END

------------------------------------
--  INSERT OPTOUT - DNT
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert OptOut - DNT')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert OptOut - DNT';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'OptOut'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[OptOut] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[OptOut] (	 
					[fkStoreID],
					[fkCustomerID],
					[fkCrumbID],
					[fkCrumbTypeID],
					[fkAutoCrumbID],
					[DateCreated],
					[Email],
					[OptOutType]
				)

				SELECT  
					'+ @StoreID +' AS fkStoreID ,
					c.pkCustomerID AS fkCustomerID ,
					0 AS fkCrumbID ,
					0 AS fkCrumbTypeID ,
					0 AS fkAutoCrumbID ,
					GETDATE() AS DateCreated ,
					ISNULL(cc.Email, '''') AS Email ,
					4 AS OptOutType --Text
				FROM [_Common].[dbo].[CommonCustomer] cc
				INNER JOIN #ImportLog i
					ON i.PKImportId = cc.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = cc.CRMCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				WHERE cc.DoNotCall = 1
				AND FKImportLogID = ' + @ImportID

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
	END

------------------------------------
--  INSERT OPTOUT - DNE
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert OptOut - DNE')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert OptOut - DNE';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'OptOut'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[OptOut] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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
				
				INSERT INTO [' + @leadcrumb + '].[dbo].[OptOut] (	 
					[fkStoreID],
					[fkCustomerID],
					[fkCrumbID],
					[fkCrumbTypeID],
					[fkAutoCrumbID],
					[DateCreated],
					[Email],
					[OptOutType]
				)

				SELECT  
					'+ @StoreID +' AS fkStoreID ,
					c.pkCustomerID AS fkCustomerID ,
					0 AS fkCrumbID ,
					0 AS fkCrumbTypeID ,
					0 AS fkAutoCrumbID ,
					GETDATE() AS DateCreated ,
					ISNULL(cc.Email, '''') AS Email ,
					1 AS OptOutType --Email
				FROM [_Common].[dbo].[CommonCustomer] cc
				INNER JOIN #ImportLog i
					ON i.PKImportId = cc.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = cc.CRMCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				WHERE cc.DoNotEmail = 1
				AND FKImportLogID = ' + @ImportID

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
	END

------------------------------------
-- INSERT OPTOUT - DNSM
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert OptOut - DNSM')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert OptOut - DNSM';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'OptOut'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[OptOut] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[OptOut] (	 
					[fkStoreID],
					[fkCustomerID],
					[fkCrumbID],
					[fkCrumbTypeID],
					[fkAutoCrumbID],
					[DateCreated],
					[Email],
					[OptOutType]
				)

				SELECT  
					'+ @StoreID +' AS fkStoreID ,
					c.pkCustomerID AS fkCustomerID ,
					0 AS fkCrumbID ,
					0 AS fkCrumbTypeID ,
					0 AS fkAutoCrumbID ,
					GETDATE() AS DateCreated ,
					ISNULL(cc.Email, '''') AS Email ,
					3 AS OptOutType --Snailmail
				FROM [_Common].[dbo].[CommonCustomer] cc
				INNER JOIN #ImportLog i
					ON i.PKImportId = cc.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Customer] c
					ON c.PersonalNotes = cc.CRMCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				WHERE cc.DoNotMail = 1
				AND FKImportLogID = ' + @ImportID

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
	END

------------------------------------
--  INSERT DEALFLAG - PROPOSAL
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Proposal')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealFlag - Proposal';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealFlag'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	 
					[fkDealID],
					[DealFlagType],
					[DateCreated],
					[DateModified],
					[IsDeleted],
					[fkStoreID]
				)

				SELECT 
					d.pkDealID,
					26,
					cd.ProposalDealFlag,
					cd.DateModified,
					0,
					'+ @StoreID +'

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog i
					ON i.PKImportId = cd.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d
					ON d.TimeFrame = cd.[CRMDealID] 
					AND ISNULL(cd.[CRMDealID],'''') <> ''''
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'

				WHERE ISNULL(cd.ProposalDealFlag,'''') <> '''' 
				AND FKImportLogID = ' + @ImportID


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
	END

------------------------------------
-- INSERT DEALFLAG - DEAD
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealFlag - Dead')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealFlag - Dead';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealFlag'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealFlag] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				IF OBJECT_ID(''tempdb..##DeadDeals'') IS NOT NULL
				DROP TABLE ##DeadDeals

				CREATE TABLE ##DeadDeals (pkDealID INT) 

				INSERT INTO [' + @leadcrumb + '].[dbo].[DealFlag] (	 
					[fkDealID],
					[DealFlagType],
					[DateCreated],
					[DateModified],
					[IsDeleted],
					[fkStoreID]
				)

				OUTPUT Inserted.fkDealID INTO ##DeadDeals

				SELECT 
					d.pkDealID,
					5,
					cd.InactiveDealFlag,
					cd.DateModified,
					0,
					'+ @StoreID +'

				FROM [_Common].[dbo].[CommonDeal] cd
				INNER JOIN #ImportLog i
					ON i.PKImportId = cd.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN [' + @leadcrumb + '].[dbo].[Deal] d
					ON d.TimeFrame = cd.[CRMDealID] 
					AND ISNULL(cd.[CRMDealID],'''') <> ''''
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'

				WHERE ISNULL(cd.InactiveDealFlag,'''') <> '''' 
				AND FKImportLogID = ' + @ImportID 

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
	END

------------------------------------
-- INSERT DEALLOG - DEAD DEALS
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert DealLog - Dead Deals')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert DealLog - Dead Deals';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'DealLog'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[DealLog] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				INSERT INTO [' + @leadcrumb + '].[dbo].[DealLog] (	 
					fkDealID, 
					fkUserID, 
					fkCustomerVehicleID, 
					fkStoreID, 
					fkCustomerID, 
					fkDealStatusID, 
					fkCrumbID, 
					fkTaskID, 
					fkAppointmentID, 
					DealLogType, 
					[Message], 
					DateCreated, 
					DateModified, 
					IsDeleted, 
					DealFlagType, 
					fkReferenceID, 
					ResultType)--, ResponseTimeSeconds, Tally
				SELECT 
					d.pkDealID, 
					d.fkUserIDSales1, 
					0, 
					d.fkstoreid, 
					d.fkcustomerid, 
					0, 
					0, 
					0, 
					0, 
					108, 
					''Import script killed the deal (ELead Create CRM Items)'', 
					DATEADD(HOUR, 12, DateCreated), 
					DateCreated, 
					0, 
					0, 
					0, 
					0
				FROM [' + @leadcrumb + '].[dbo].[Deal] d
				JOIN ##DeadDeals dd ON d.pkDealID = dd.pkDealID'

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
	END

------------------------------------
--  INSERT CRUMB - EMAILS
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert Crumb - Emails')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert Crumb - Emails';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);
	
			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'Crumb'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[Crumb] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				IF OBJECT_ID(''tempdb..#Customer'') IS NOT NULL
					DROP TABLE #Customer
				SELECT * INTO #Customer FROM [' + @leadcrumb + '].[dbo].[Customer] WHERE fkStoreID = '+ @StoreID +'		
				CREATE NONCLUSTERED INDEX [ix_customer_personalnotes] ON #Customer ([fkStoreID]) INCLUDE ([pkCustomerID],[PersonalNotes])


				IF OBJECT_ID(''tempdb..#Deal'') IS NOT NULL
					DROP TABLE #Deal
				SELECT * INTO #Deal from [' + @leadcrumb + '].[dbo].[Deal] WHERE fkStoreID = '+ @StoreID +'
				CREATE NONCLUSTERED INDEX [ix_deal_pkdealid] ON #Deal ([fkStoreID]) INCLUDE ([pkDealID],[TimeFrame],[ADFXML])
				CREATE NONCLUSTERED INDEX [ix_deal_pkdealid_timeframe] ON #Deal ([fkStoreID],[TimeFrame]) INCLUDE ([pkDealID],[ADFXML])

				INSERT INTO [' + @leadcrumb + '].[dbo].[Crumb] (	
					[fkReferenceID],
					[fkReferenceType],
					[fkUserID],
					[CrumbType],
					[Subject],
					[IsSent],
					[DateCreated],
					[DateModified],
					[fkCrumbTemplateID],
					[GUID],
					[CDyneTextID],
					[fkAutoCrumbID],
					[fkAutoCrumbNodeID],
					[fkStoreID],
					[fkMassCrumbID],
					[IsRead],
					[fkReplyCrumbID],
					[fkDealID],
					[CompressedMessage],
					[fkForwardedFromUserID],
					[IsStarred],
					[CC],
					[BCC],
					[From],
					[To],
					[FromName],
					[MediaGUID],
					[ExtendedID],
					[StrippedMessage],
					[EmailSenderType],
					[fkReadByUserID],
					[DateRead],
					[SurveyCategory],
					[HasPhoto],
					[HasVideo],
					[fkServiceLeadID],
					[UnicodeStrippedMessage],
					[fkImpersonatedByUserID]
				)

				SELECT 
					ISNULL(c.pkCustomerID,0),
					1,
					ISNULL(u.pkUserID,0),
					CrumbType,
					cc.[Subject],
					1,
					CrumbDateCreated,
					CrumbDateModified,
					0,
					NEWID() AS GUID,
					'''' as CDyneTextID,
					0 as fkAutoCrumbID,
					0 as fkAutoCrumbNodeID,
					'+ @StoreID +',
					0 as fkMassCrumbID,
					1 as IsRead,
					0 as fkReplyCrumbID,
					ISNULL(d.pkDealID,0),
					0,
					'''',
					'''',
					'''',
					'''',
					cc.[From],
					cc.[To],
					'''',
					0,
					0,
					cc.[StrippedMessage],
					0,
					0,
					cc.[DateRead],
					0,
					0,
					0,
					0,
					cc.[UnicodeStrippedMessage],
					0

				FROM [_Common].[dbo].[CommonCrumb] cc
				INNER JOIN #ImportLog i
					ON i.PKImportId = cc.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN #Customer c
					ON c.PersonalNotes = cc.fkCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN #Deal d
					ON d.TimeFrame = cc.[FKDealID] 
					AND ISNULL(cc.[FKDealID],'''') <> ''''
					AND d.ADFXML = CASE 
										WHEN sc.[Name] = ''Elead'' THEN ''THIS IS A ELEADS PROSPECT IMPORT''
										WHEN sc.[Name] = ''Vin'' THEN ''THIS IS A VIN PROSPECT IMPORT''
										ELSE ''''
									END
					AND d.fkStoreID = '+ @StoreID +'
				INNER JOIN [' + @leadcrumb + '].[dbo].[User] u
					ON u.[Biography] = cc.fkUserID
					AND ISNULL(u.[Biography],'''') <> ''''
					AND u.fkStoreID = '+ @StoreID +'

				WHERE ISNULL(cc.CrumbType,'''') IN (''10'', ''15'', ''19'')
				AND FKImportLogID = ' + @ImportID



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
	END

------------------------------------
--  INSERT CRUMB - TEXTS
------------------------------------
SELECT @CompletionCheck = [_Common].[dbo].fnIsImportSegmentCompleted (@GUID, 'CommontoProd', 'Insert Crumb - Texts')

IF @CompletionCheck = 0
	BEGIN
		SET @ChildStageName = 'Insert Crumb - Texts';

			--Pull Child Stage ID
			SET @ChildStage = (SELECT pkImportChildStageID FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].[ImportChildStage] WHERE fkImportParentStageID = @ParentStage AND ChildStageName = @ChildStageName);

			--Set Leadcrumb table name and count
			SET @LeadcrumbTable = 'Crumb'
	
			SET @sqlcmd = N' SELECT @TableCountDynamic = COUNT(*) FROM [' + @leadcrumb + '].[dbo].[Crumb] WITH (NOLOCK)'
			EXEC sp_executesql @sqlcmd, N'@TableCountDynamic INT OUT', @LeadcrumbTableStartCount OUT
			

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

				IF OBJECT_ID(''tempdb..#Customer'') IS NOT NULL
					DROP TABLE #Customer
				SELECT * INTO #Customer FROM [' + @leadcrumb + '].[dbo].[Customer] WHERE fkStoreID = '+ @StoreID +'		
				CREATE NONCLUSTERED INDEX [ix_customer_personalnotes] ON #Customer ([fkStoreID]) INCLUDE ([pkCustomerID],[PersonalNotes])


				INSERT INTO [' + @leadcrumb + '].[dbo].[Crumb] (	
					[fkReferenceID],
					[fkReferenceType],
					[fkUserID],
					[CrumbType],
					[Subject],
					[IsSent],
					[DateCreated],
					[DateModified],
					[fkCrumbTemplateID],
					[GUID],
					[CDyneTextID],
					[fkAutoCrumbID],
					[fkAutoCrumbNodeID],
					[fkStoreID],
					[fkMassCrumbID],
					[IsRead],
					[fkReplyCrumbID],
					[fkDealID],
					[CompressedMessage],
					[fkForwardedFromUserID],
					[IsStarred],
					[CC],
					[BCC],
					[From],
					[To],
					[FromName],
					[MediaGUID],
					[ExtendedID],
					[StrippedMessage],
					[EmailSenderType],
					[fkReadByUserID],
					[DateRead],
					[SurveyCategory],
					[HasPhoto],
					[HasVideo],
					[fkServiceLeadID],
					[UnicodeStrippedMessage],
					[fkImpersonatedByUserID]
				)

				SELECT 
					ISNULL(c.pkCustomerID,0),
					1,
					ISNULL(u.pkUserID,0),
					CrumbType,
					'''',
					1,
					CrumbDateCreated,
					CrumbDateModified,
					0,
					NEWID() AS GUID,
					'''' as CDyneTextID,
					0 as fkAutoCrumbID,
					0 as fkAutoCrumbNodeID,
					'+ @StoreID +',
					0 as fkMassCrumbID,
					cc.IsRead,
					0 as fkReplyCrumbID,
					0,
					0,
					'''',
					'''',
					'''',
					'''',
					cc.[From],
					cc.[To],
					'''',
					0,
					0,
					cc.[StrippedMessage],
					0,
					0,
					cc.[DateRead],
					0,
					0,
					0,
					0,
					cc.[UnicodeStrippedMessage],
					0

				FROM [_Common].[dbo].[CommonCrumb] cc
				INNER JOIN #ImportLog i
					ON i.PKImportId = cc.FKImportLogID
				INNER JOIN #CrmImportSourceConfig sc
					ON i.FKCRMID = sc.CrmImportSourceConfigId
				INNER JOIN #Customer c
					ON c.PersonalNotes = cc.fkCustomerID
					AND ISNULL(c.PersonalNotes,'''') <> ''''
					AND c.fkStoreID = '+ @StoreID +'
				INNER JOIN [' + @leadcrumb + '].[dbo].[User] u
					ON u.[Biography] = cc.fkUserID
					AND ISNULL(u.[Biography],'''') <> ''''
					AND u.fkStoreID = '+ @StoreID +'

				WHERE ISNULL(cc.CrumbType,'''') IN (''33'', ''2'')
				AND FKImportLogID = ' + @ImportID



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
	END

------------------------------------
--  END OF SCRIPT SUCCESS MESSAGE
------------------------------------
SET @ChildStageName = 'CommontoProd Successfully Completed';

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
		PRINT ''CommontoProd has completed successfully.  Please check ImportHistory and ImportError for details'''


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
