SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[Master_StagingtoCommon]   


	@GUID UNIQUEIDENTIFIER

AS



DECLARE @ImportID VARCHAR(MAX)
DECLARE @StoreID VARCHAR(5) 
DECLARE @IP VARCHAR(50)
DECLARE @leadcrumb VARCHAR(50)
DECLARE @CRM VARCHAR(50)
DECLARE @ImportType VARCHAR(100)


SET NOCOUNT ON

SELECT 
@ImportID = pkImportID,
@StoreID = FKStoreID,
@CRM = sc.[Name],
@ImportType = it.[Type]
FROM [StoreOnboardingAutomation].[dbo].[ImportLog] il
INNER JOIN [StoreOnboardingAutomation].[dbo].CRMImportSourceConfig sc
	ON il.FKCRMID = sc.CRMImportSourceConfigId
INNER JOIN [StoreOnboardingAutomation].[dbo].[ImportType] it
	ON il.ImportType = it.PKImportTypeId	
WHERE GUID = @GUID
	AND (DateCompleted = '' OR DateCompleted IS NULL)


IF @CRM = 'ELead' AND @ImportType = 'Initial'
BEGIN
	EXEC [StoreOnboardingAutomation].[dbo].StagingtoCommon_Eleads @GUID
END


IF @CRM = 'Vin' AND @ImportType = 'Initial'
BEGIN
	EXEC [StoreOnboardingAutomation].[dbo].StagingtoCommon_Vin @GUID
END



