/****** 
New Store pkID: 32
******/

USE [StoreOnboardingAutomation]

SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[StoreDecom_Galaxy] 
	@StoreID int

AS
SET NOCOUNT ON;
-------------------------------------------------------------------
-- VARIABLES
-------------------------------------------------------------------

DECLARE @ErrorMessage  NVARCHAR(4000), 
		@ErrorSeverity INT, 
		@ErrorState    INT

BEGIN
	BEGIN TRY
		BEGIN TRANSACTION
----------------------------------------------------------------------
--  Soft-Delete the store record
----------------------------------------------------------------------
			UPDATE  [Galaxy].[Galaxy].[dbo].[Store]
			SET     IsDeleted = 1,
					DateModified = current_timestamp
			WHERE  pkStoreID = @StoreID ;
 
----------------------------------------------------------------------
--  Archive and DELETE StoreAttribute Records
----------------------------------------------------------------------
			INSERT INTO [Galaxy].[Galaxy].[dbo].[StoreAttribute_Archive]
						SELECT  *
						FROM    [Galaxy].[Galaxy].[dbo].[StoreAttribute]
						WHERE   fkStoreID = @StoreID ;
  
			DELETE FROM [Galaxy].[Galaxy].[dbo].[StoreAttribute]
			WHERE       fkStoreID = @StoreID ;
 
----------------------------------------------------------------------
--  Archive and DELETE StoreGroup Records
----------------------------------------------------------------------
			INSERT INTO [Galaxy].[Galaxy].[dbo].[StoreGroup_Archive]
						SELECT  *
						FROM    [Galaxy].[Galaxy].[dbo].[StoreGroup]
						WHERE   fkStoreID = @StoreID ;
  
			DELETE FROM [Galaxy].[Galaxy].[dbo].[StoreGroup]
			WHERE      fkStoreID = @StoreID ;
 
 
----------------------------------------------------------------------
--  Remove DriveServerLink Record
----------------------------------------------------------------------
			DELETE FROM [Galaxy].[Galaxy].[dbo].[StoreDriveServerLink]
			WHERE       fkStoreID = @StoreID ;

		COMMIT;
	END TRY

	BEGIN CATCH
			IF @@TRANCOUNT > 0  
				ROLLBACK TRANSACTION;  

			SELECT 
				@ErrorMessage = ERROR_MESSAGE(), 
				@ErrorSeverity = ERROR_SEVERITY(), 
				@ErrorState = ERROR_STATE();

			-- return the error inside the CATCH block
			RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);

	END CATCH
END