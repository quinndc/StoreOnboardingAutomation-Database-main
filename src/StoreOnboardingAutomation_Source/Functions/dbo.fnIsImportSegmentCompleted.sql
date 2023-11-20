SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION [dbo].[fnIsImportSegmentCompleted]
(
	@GUID UNIQUEIDENTIFIER,
	@ParentStageName VARCHAR(100),
	@ChildStageName VARCHAR(100)
)
RETURNS INT

-- 0 is Import Segment has not completed
-- 1 is Import Segment has completed

BEGIN

	RETURN
			CASE				
				WHEN 
					ISNULL(@ParentStageName,'') <> '' AND ISNULL(@ChildStageName,'') = '' AND
					EXISTS (SELECT 1 FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].vwGetImportLogStages 
						WHERE [GUID] = @GUID
						AND ParentStageName = @ParentStageName
						AND ChildStageName LIKE '%Completed%'
						AND StageEnd IS NOT NULL) 				
				THEN 1 

				WHEN 
					ISNULL(@ParentStageName,'') <> '' AND ISNULL(@ChildStageName,'') <> '' AND
					EXISTS (SELECT 1 FROM [PRDENCMS001].[StoreOnboardingAutomation].[dbo].vwGetImportLogStages 
						WHERE [GUID] = @GUID
						AND ParentStageName = @ParentStageName
						AND ChildStageName = @ChildStageName
						AND StageEnd IS NOT NULL) 				
				THEN 1 

				ELSE 0
			END


	RETURN 0
END