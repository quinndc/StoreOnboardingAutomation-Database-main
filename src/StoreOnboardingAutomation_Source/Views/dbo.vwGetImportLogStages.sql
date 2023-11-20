SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE VIEW [dbo].[vwGetImportLogStages]
AS
SELECT
    il.pkImportID,
    il.GUID,
    il.fkStoreID ,
    ParentStageName,
    ChildStageName,
    ih.StageStart,
    ih.StageEnd,
    DATEDIFF(SECOND, ih.StageStart, ih.StageEnd) Seconds,
    CONVERT(VARCHAR, (DATEDIFF(SECOND, ih.StageStart, ih.StageEnd)) / (60 * 60 * 24)) + ':'
    + CONVERT(VARCHAR, DATEADD(s, (DATEDIFF(SECOND, ih.StageStart, ih.StageEnd)), CONVERT(DATETIME2, '0001-01-01')), 108) TimeSpan,
    ih.[RowCount],
    CASE
    WHEN ie.ErrorMessage IS NULL AND ih.StageEnd IS NULL AND ih2.pkImportHistoryID IS NULL THEN 'Running'
    WHEN ie.ErrorMessage IS NULL AND ih. StageEnd IS NULL AND ih2.pkImportHistoryID IS NOT NULL AND ih.fkImportChildStageID = ih2.fkImportChildStageID THEN 'Manually Stopped' 
    WHEN ie.ErrorMessage IS NOT NULL AND ih.StageEnd IS NULL THEN ie.ErrorMessage 
    ELSE 'Completed without error'
    END AS Error_Status,
    ih.TableName,
    ih.TableStartCount

FROM [PRDENCMS001].[StoreOnboardingAutomation].dbo.ImportHistory ih
INNER JOIN [PRDENCMS001].[StoreOnboardingAutomation].dbo.importparentstage ps
    ON ps.pkimportparentstageid = ih.fkImportParentStageID
LEFT JOIN [PRDENCMS001].[StoreOnboardingAutomation].dbo.ImportChildStage cs
    ON cs.pkImportChildStageID = ih.fkImportChildStageID
INNER JOIN [PRDENCMS001].[StoreOnboardingAutomation].dbo.ImportLog il
    ON il.pkImportID = ih.fkImportID
LEFT JOIN [PRDENCMS001].[StoreOnboardingAutomation].dbo.ImportHistory ih2
    ON ih.pkImportHistoryID + 1 = ih2.pkImportHistoryID
LEFT JOIN [PRDENCMS001].[StoreOnboardingAutomation].dbo.ImportErrorLog ie
    ON ie.fkimporthistoryid = ih.pkimporthistoryid
    
GO