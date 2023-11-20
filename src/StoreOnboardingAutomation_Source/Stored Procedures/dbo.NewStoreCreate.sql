SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE PROCEDURE [dbo].[NewStoreCreate]   
	@DriveServerID INT,
	@StoreName VARCHAR(128),
	@StoreAddress1 VARCHAR(128),
	@StoreCity VARCHAR(32),
	@StoreState VARCHAR(2),
	@StoreZip VARCHAR(10),
	@CaddyLogin VARCHAR(128)

 


AS


-- these four variables will be automatically populated from the DriveServerID so populating them is optional.
DECLARE @GroupName varchar(128) 
DECLARE @DatabaseName varchar(50)  
DECLARE @DatabaseIP varchar(16) 
DECLARE @RootURL varchar(50)




------------------------------------------------------------------------------------------------
-- DO NOT ALTER ANYTHING BELOW THIS LINE
------------------------------------------------------------------------------------------------

IF (@DriveServerID = 0)
BEGIN
	raiserror('@DriveServerID CANNOT BE SET TO 0', 20, -1) with log 
END


SELECT TOP 1 
	@DatabaseName = DataBaseName,
	@DatabaseIP = InternalServerIP,
	@GroupName = [Description],
	@RootURL = RootURL
FROM	[galaxy].Galaxy.dbo.DriveServer 	
WHERE	pkDriveServerID = @DriveServerID

DECLARE @StoreID int = 0
DECLARE @StoreIDString varchar(4)
DECLARE @CurrentGroupName varchar(16)
DECLARE @CurrentStarName varchar(16)
SELECT @CurrentGroupName = 'Star 0' + CONVERT(VARCHAR(4), @DriveServerID)
SELECT @CurrentStarName = REPLACE(@CurrentGroupName, ' ', '')
DECLARE @StoreGUID uniqueidentifier = NEWID() -- do this here so it can be used for matching to get pkStoreID

-- features
DECLARE @HasFeatureCaddy bit = 1
DECLARE @HasFeatureVideo bit = 1  --default 1
DECLARE @HasFeatureDesking bit = 0
DECLARE @HasFeatureSnap bit = 0
DECLARE @HasFeatureReputationManagement bit = 0
DECLARE @HasFeatureOutboundCalling bit = 0
DECLARE @HasFeatureScout bit = 0
DECLARE @HasFeatureServiceLeads bit = 0
DECLARE @HasFeatureServiceRO bit = 0

INSERT INTO [galaxy].Galaxy.dbo.Store (
	[Name],
    Address1 ,
    Address2 ,
    City ,
    [State],
    Zip ,
    Email ,
    Phone ,
    DateCreated ,
    DateModified ,
    IsDeleted ,
    HomeNetDealerID ,
    WebsiteURL ,
    [GUID],
    HomeNetFilePrefix ,
    VAutoDealerID ,
    LeadEmail ,
    GuestConceptsID ,
    ReynoldsEmail ,
    CallSourceID ,
    CallSourceLeadEmail ,
    Logo ,
    Nickname,
    StoreStatus ,
    IsEnterprise)
VALUES ( 
	@StoreName, -- Name - varchar(128)
    @StoreAddress1, -- Address1 - varchar(128)
    '' , -- Address2 - varchar(128)
    @StoreCity, -- City - varchar(32)
    @StoreState, -- State - varchar(2)
    @StoreZip, -- Zip - varchar(10)
    '' , -- Email - varchar(256)
    '' , -- Phone - varchar(16)
    GETDATE() , -- DateCreated - datetime
    GETDATE() , -- DateModified - datetime
    0 , -- IsDeleted - bit
    0 , -- HomeNetDealerID - int
    '' , -- WebsiteURL - varchar(256)
    @StoreGUID, -- GUID - uniqueidentifier
    '' , -- HomeNetFilePrefix - varchar(128)
    '' , -- VAutoDealerID - varchar(32)
    '' , -- LeadEmail - varchar(256)
    '' , -- GuestConceptsID - varchar(50)
    '' , -- ReynoldsEmail - varchar(max)
    '' , -- CallSourceID - varchar(max)
    '' , -- CallSourceLeadEmail - varchar(max)
    '' , -- Logo - varchar(max)
    '' , -- Nickname - varchar(max)
    1  , --StoreStatus
    0    --IsEnterprise
)


-- not running this on galaxy so can't use local SCOPE_IDENTITY...
--SELECT @StoreID = SCOPE_IDENTITY()
-- ...so do this instead.
SELECT @StoreID = MAX(pkStoreID) FROM [galaxy].Galaxy.dbo.Store 
WHERE [Name] = @StoreName AND IsDeleted = 0 and [GUID] = @StoreGUID

SELECT @StoreIDString = CONVERT(VARCHAR(4), @StoreID)

-- dynamic sql for local store creation
EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.LocalStore ( 
	fkStoreID ,
	Name ,
	Address1 ,
	Address2 ,
	City ,
	State ,
	Zip ,
	Email ,
	Phone ,
	DateCreated ,
	DateModified ,
	IsDeleted ,
	HomeNetDealerID ,
	WebsiteURL ,
	GUID ,
	HomeNetFilePrefix ,
	VAutoDealerID ,
	LeadEmail ,
	GuestConceptsID ,
	ReynoldsEmail ,
	CallSourceID ,
	CallSourceLeadEmail ,
	Logo ,
	Nickname ,
	StoreStatus ,
	IsEnterprise)
SELECT pkStoreID ,
    Name ,
    Address1 ,
    Address2 ,
    City ,
    State ,
    Zip ,
    Email ,
    Phone ,
    DateCreated ,
    DateModified ,
    IsDeleted ,
    HomeNetDealerID ,
    WebsiteURL ,
    GUID ,
    HomeNetFilePrefix ,
    VAutoDealerID ,
    LeadEmail ,
    GuestConceptsID ,
    ReynoldsEmail ,
    CallSourceID ,
    CallSourceLeadEmail ,
    Logo ,
    Nickname ,
    StoreStatus ,
    IsEnterprise
FROM [galaxy].Galaxy.dbo.Store 
WHERE pkStoreID = ' + @StoreIDString)

-- set the currentstar on the local db
EXEC ('UPDATE [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.CurrentStar SET Name = ''' + @CurrentStarName + '''')

--Insert into StoreDriveServerLink (fkStoreID from Store and fkDriveServerID from DriveServer)
insert into [galaxy].Galaxy.dbo.StoreDriveServerLink (fkStoreID,fkDriveServerID) VALUES (@StoreID, @DriveServerID)

--Insert into StoreGroup (Take the fkStoreID, create new id, Store name, NEWID())
INSERT INTO [galaxy].Galaxy.dbo.StoreGroup (fkStoreID,fkGroupID,Name,[GUID]) VALUES (@StoreID, @DriveServerID, @GroupName, NEWID())


/*
-- migrate biz rules from template store
EXEC [galaxy].Galaxy.dbo.[_StoreWizardMegaMigration]
    @FromStoreID = 38,  --Midnight Drive's template
    @ToStoreID = @StoreID  --New Store
*/

 
 
-- caddy
IF (@HasFeatureCaddy = 1)   -- @HasFeatureCaddy bit = 1
BEGIN

	DECLARE @CaddyGalaxyUserID int = 0
	DECLARE @CaddyStarUserID int = 0
	DECLARE @CaddyGalaxyUserGUID uniqueidentifier = NEWID() -- do this here so it can be used for matching to get pkStoreID

	-- create caddy user on galaxy table
	INSERT	[galaxy].Galaxy.dbo.[GalaxyUser]
	(
		[GUID], 
		[fkSessionDriveServerID], 
		[Email], 
		[Password], 
		[FirstName], 
		[LastName], 
		[IsActive], 
		[UserType], 
		[DateCreated], 
		[DateModified], 
		[IsDeleted]
	)
	VALUES
	(
		@CaddyGalaxyUserGUID, -- NEWID()
		@DriveServerID, 
		@CaddyLogin, 
		'TotallyBogusPasswordThatWontLetYouIn', 
		'Shelby', 
		'Parker', 
		1, 
		80, 
		CURRENT_TIMESTAMP, 
		CURRENT_TIMESTAMP, 
		0
	)
	
	-- not running this on galaxy so can't use local SCOPE_IDENTITY...
	--	SELECT @CaddyGalaxyUserID = SCOPE_IDENTITY()
	-- ...so do this instead.
	SELECT @CaddyGalaxyUserID = MAX(pkGalaxyUserID) FROM [galaxy].Galaxy.dbo.GalaxyUser 
	WHERE [Email] = @CaddyLogin AND fkSessionDriveServerID = @DriveServerID AND IsDeleted = 0 and [GUID] = @CaddyGalaxyUserGUID
		
	--SELECT @CaddyGalaxyUserGUID = [GUID] FROM [galaxy].Galaxy.dbo.GalaxyUser WHERE pkGalaxyUserID = @CaddyGalaxyUserID

	DECLARE @CreateUserSql nVARCHAR(MAX) = 
	'INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.[User] ( 
		fkStoreID ,
		Email ,
		[Password] ,
		FirstName ,
		LastName ,
		Title ,
		UserType ,
		DateCreated ,
		DateModified ,
		IsDeleted ,
		IsActive ,
		SessionStoreID ,
		[GUID] ,
		ForwardEmailTo ,
		CellPhone,
		GalaxyUserGUID
	)
	VALUES (  
		' + @StoreIDString + ',
		''' + @CaddyLogin +''',
		'''',
		''Shelby'',
		''Parker'',
		'''',
		80 ,
		GETDATE() ,
		GETDATE() ,
		0 ,
		1 ,
		' + @StoreIDString + ',
		NEWID() ,
		'''',
		'''',
		''' + CONVERT(VARCHAR(38), @CaddyGalaxyUserGUID) + ''')'

	DECLARE @CaddyUserID int = 0
	EXECUTE sp_executesql @CreateUserSql, N'@CaddyUserID INTEGER OUTPUT', @CaddyUserID OUTPUT


/* doesn't work anymore
	EXEC [galaxy].Galaxy.dbo.CaddyMigrationFromPreviousVersion
		@FromStoreID = 38,  --Midnight Drive's template
		@ToStoreID = @StoreID,  --New Store
 
		@ToCaddyUserID    = @CaddyUserID,  --New Caddy user
		@FromCaddyUserID = 7852  --Midnight Drive's Caddy
*/

END



IF (@HasFeatureReputationManagement = 1)  -- @HasFeatureReputationManagement bit = 0
BEGIN
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''ReputationManagement'',
		''True'',
		GETDATE(),
		GETDATE(),
		0)')
END

IF (@HasFeatureDesking = 1)  -- @HasFeatureDesking bit = 0
BEGIN
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''Desking'',
		''True'',
		GETDATE(),
		GETDATE(),
		0)')
END

IF (@HasFeatureSnap = 1)  -- @HasFeatureSnap bit = 0
BEGIN
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''Snap'',
		''True'',
		GETDATE(),
		GETDATE(),
		0)')
END

IF (@HasFeatureScout = 1)  -- @HasFeatureScout bit = 0
BEGIN
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''Scout'',
		''True'',
		GETDATE(),
		GETDATE(),
		0)')
END

IF (@HasFeatureServiceLeads = 1)  --  @HasFeatureServiceLeads bit = 0
BEGIN
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''ServiceLeads'',
		''True'',
		GETDATE(),
		GETDATE(),
		0)')
END

IF (@HasFeatureServiceRO = 1)   -- @HasFeatureServiceRO bit = 0
BEGIN
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''ServiceClosedRO'',
		''True'',
		GETDATE(),
		GETDATE(),
		0)')
END

IF (@HasFeatureVideo = 1)  -- @HasFeatureVideo bit = 1
BEGIN
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''SendVideos'',
		''True'',
		GETDATE(),
		GETDATE(),
		0)')

	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''SendVideosWeb'',
		''True'',
		GETDATE(),
		GETDATE(),
		0)')
END

IF (@HasFeatureOutboundCalling = 1)  -- @HasFeatureOutboundCalling bit = 0
BEGIN
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StoreFeature ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		' + @StoreIDString + ',
		''ClickToCall'',
		''Enabled'',
		GETDATE(),
		GETDATE(),
		0)')
END



-- default email signature should be copied from midnight drive
INSERT INTO [galaxy].Galaxy.dbo.StoreAttribute (fkStoreID, [Key], [Value], DateCreated, DateModified, IsDeleted)
SELECT @StoreID, [Key], [Value], CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0
FROM [galaxy].Galaxy.dbo.StoreAttribute
WHERE fkStoreID=38 
	and [Key]='DefaultEmailSignature'
	and IsDeleted = 0

-- ON-2467 Reynolds default setting
INSERT INTO [galaxy].Galaxy.dbo.StoreAttribute (fkStoreID, [Key], [Value], DateCreated, DateModified, IsDeleted)
VALUES(@StoreID, 'ReynoldsDmsInventoryUpdateOnly', 'True', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
 

-- ---------------------------------------------------------------------------------
-- business rules logic from Galaxy.dbo._StoreWizardMegaMigration stored procedure.
-- ---------------------------------------------------------------------------------

	DECLARE @FromStoreID INT, @ToStoreID INT

    SET @FromStoreID = 38  	--Midnight Drive's template
    SET @ToStoreID = @StoreID  --New Store


	DECLARE	@FromServerIP	VARCHAR(MAX) = (SELECT TOP 1 ds.InternalServerIP from	[galaxy].Galaxy.dbo.DriveServer ds	INNER JOIN [galaxy].Galaxy.dbo.StoreDriveServerLink sds ON ds.pkDriveServerID = sds.fkDriveServerID AND sds.fkStoreID = @FromStoreID)
	DECLARE	@FromServerName	VARCHAR(MAX) = (SELECT TOP 1 ds.DataBaseName from	[galaxy].Galaxy.dbo.DriveServer ds	INNER JOIN [galaxy].Galaxy.dbo.StoreDriveServerLink sds ON ds.pkDriveServerID = sds.fkDriveServerID AND sds.fkStoreID = @FromStoreID)

	DECLARE	@ToServerIP		VARCHAR(MAX) = (SELECT TOP 1 ds.InternalServerIP from	[galaxy].Galaxy.dbo.DriveServer ds	INNER JOIN [galaxy].Galaxy.dbo.StoreDriveServerLink sds ON ds.pkDriveServerID = sds.fkDriveServerID AND sds.fkStoreID = @ToStoreID)
	DECLARE	@ToServerName	VARCHAR(MAX) = (SELECT TOP 1 ds.DataBaseName from	[galaxy].Galaxy.dbo.DriveServer ds	INNER JOIN [galaxy].Galaxy.dbo.StoreDriveServerLink sds ON ds.pkDriveServerID = sds.fkDriveServerID AND sds.fkStoreID = @ToStoreID)

	DECLARE	@FromServerTablePrefix VARCHAR(MAX) = '[' + @FromServerIP + '].' + @FromServerName + '.dbo.'
	DECLARE	@ToServerTablePrefix VARCHAR(MAX) = '[' + @ToServerIP + '].' + @ToServerName + '.dbo.'

	DECLARE @StoreIDTo int = @ToStoreID
	DECLARE @StoreIDFrom int = @FromStoreID

	-- copy business rules
	
	-- NOTE that I don't think this has been working for a while, the fkStoreID filters will never be true, haha
	DECLARE @BizRuleSql VARCHAR(MAX) = 
	'	INSERT INTO  ToBusinessRule (fkStoreID,Name,Description,TriggerType,EventType,DelayMinutes,TaskDueMinutes,IsActive,DateCreated,DateModified,IsDeleted,fkCrumbTemplateID,NewUsedType,TimeOfDay,IsIgnoredIfApptSetForFuture,IsIgnoredIfApptShownInPast,IsIgnoredIfCustomerSentEmail,IsIgnoredIfDealIsSold,IsIgnoredIfDealIsDead,IsIgnoredIfDealIsDelivered,IsIgnoredIfPhoneConversation,IsIgnoredIfNoVehicleInformation,AssignmentType,MileageStart,MileageEnd,IneligibleOpCode,TurnDownCode,IsFirstSurvey,IsGoodLastSurveyResponse,IsBadLastSurveyResponse,IsNoneLastSurveyResponse,IsAllowedToBeCancelled,IsAllowedToRunOutsideBusinessHours,fkSurveyID,IsIgnoredIfUserSentManualEmail,DealSourceSubType,DealSourceType,MinimumFollowUp,ExecutionType,fkCustomReportID,ChangeToCaddyStage,IsIgnoredIfCustomerHasValidPhoneNumber,IsIgnoredIfCustomerHasNoPhoneNumber,CaddyHiringTriggerType,WhenToRunType,IsIgnoredIfPhoneCallMade,IsIgnoredIfUserSentText,IsIgnoredIfCustomerTextedBack,IsIgnoredIfCustomerHasEmail,IsIgnoredIfCustomerHasNoEmail,TouchPointActionTypes)
		SELECT	@StoreIDTo,[Name],[Description],TriggerType,EventType,DelayMinutes,TaskDueMinutes,IsActive,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,IsDeleted,fkCrumbTemplateID,NewUsedType,TimeOfDay,IsIgnoredIfApptSetForFuture,IsIgnoredIfApptShownInPast,IsIgnoredIfCustomerSentEmail,IsIgnoredIfDealIsSold,IsIgnoredIfDealIsDead,IsIgnoredIfDealIsDelivered,IsIgnoredIfPhoneConversation,IsIgnoredIfNoVehicleInformation,AssignmentType,MileageStart,MileageEnd,IneligibleOpCode,TurnDownCode,IsFirstSurvey,IsGoodLastSurveyResponse,IsBadLastSurveyResponse,IsNoneLastSurveyResponse,IsAllowedToBeCancelled,IsAllowedToRunOutsideBusinessHours,fkSurveyID,IsIgnoredIfUserSentManualEmail,DealSourceSubType,DealSourceType,MinimumFollowUp,ExecutionType,fkCustomReportID,ChangeToCaddyStage,IsIgnoredIfCustomerHasValidPhoneNumber,IsIgnoredIfCustomerHasNoPhoneNumber,CaddyHiringTriggerType,WhenToRunType,IsIgnoredIfPhoneCallMade,IsIgnoredIfUserSentText,IsIgnoredIfCustomerTextedBack,IsIgnoredIfCustomerHasEmail,IsIgnoredIfCustomerHasNoEmail,TouchPointActionTypes
		FROM	FromBusinessRule
		WHERE	fkStoreID=@StoreIDFrom and IsDeleted=0 and isActive = 1 and TriggerType < 4900 and eventtype <> 5000  and isdeleted=0  and eventtype not in (301) and triggertype<>1002'
	--and isActive=1 
	SET @BizRuleSql = REPLACE(@BizRuleSql, 'FromBusinessRule', @FromServerTablePrefix + 'BusinessRule')
	SET @BizRuleSql = REPLACE(@BizRuleSql, 'ToBusinessRule', @ToServerTablePrefix + 'BusinessRule')
	SET @BizRuleSql = REPLACE(@BizRuleSql, '@StoreIDTo', @StoreIDTo)
	SET @BizRuleSql = REPLACE(@BizRuleSql, '@StoreIDFrom', @StoreIDFrom)
	--SELECT @BizRuleSql
	--------------------------
	--  EXEC / INSERT BUSINESS RULES
	--------------------------
	EXEC (@BizRuleSql)
	-----------------------------
	--  Copy BusinessRule Users
	-----------------------------
	DECLARE	@BizRuleUsersSQL VARCHAR(MAX) =
	'INSERT INTO ToBusinessRuleUser (fkBusinessRuleID, UserType, fkUserID, DateCreated, DateModified, IsDeleted, fkDistributionID)
	 SELECT	
			br2.pkbusinessruleid,
			UserType,
			0,
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP,
			0,
			0
	from   FromBusinessRuleUser bru
			left join FromBusinessrule br on br.pkbusinessruleid=bru.fkbusinessruleid
		    left join Tobusinessrule br2 on br2.[name] = br.[name] and br2.delayminutes=br.delayminutes and br2.triggertype=br.triggertype and br2.fkstoreid=@StoreIDTo
	where fkDistributionID=0 and UserType <> 4 and UserType <> 5 and 
			fkBusinessRuleID in (select pkBusinessRuleID from FromBusinessRule where fkStoreID=@StoreIDFrom and isactive=1)
			AND br2.pkbusinessruleid is not null
			AND not exists (select * from ToBusinessRuleUser where fkbusinessruleid = br2.pkbusinessruleid)'
	SET @BizRuleUsersSQL = REPLACE(@BizRuleUsersSQL, 'ToBusinessRule', @ToServerTablePrefix + 'BusinessRule')
	SET @BizRuleUsersSQL = REPLACE(@BizRuleUsersSQL, 'ToBusinessRuleUser', @ToServerTablePrefix + 'BusinessRuleUser')
	SET @BizRuleUsersSQL = REPLACE(@BizRuleUsersSQL, 'FromBusinessRule', @FromServerTablePrefix + 'BusinessRule')
	SET @BizRuleUsersSQL = REPLACE(@BizRuleUsersSQL, 'FromBusinessRuleUser', @FromServerTablePrefix + 'BusinessRuleUser')
	SET @BizRuleUsersSQL = REPLACE(@BizRuleUsersSQL, '@StoreIDTo', @StoreIDTo)
	SET @BizRuleUsersSQL = REPLACE(@BizRuleUsersSQL, '@StoreIDFrom', @StoreIDFrom)
	--SELECT @BizRuleUsersSQL
	--------------------------
	--  EXEC / INSERT BUSINESSRuleUSERS
	--------------------------
	EXEC (@BizRuleUsersSQL)
	--------------------------
	--  Copy Crumb Templates
	--------------------------
	DECLARE	@CrumbTemplateSQL VARCHAR(MAX) =
	'	insert into ToCrumbTemplate (fkStoreID, CrumbType, [Subject], [Message], DateCreated, DateModified, fkAttachedSurveyID, RuleIneligibleOpCode, RuleMileageMin, RuleMileageMax, BrowserHTML, PreCrumbTemplateID, PreSurveyID, PreSurveyRatingMax, PreSurveyRatingMin, PreSurveyComment, Thumbnail, TurnDownCode, [Name], SurveyCategory)
		select @StoreIDTo, CrumbType, [Subject], [Message], CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, fkAttachedSurveyID, RuleIneligibleOpCode, RuleMileageMin, RuleMileageMax, BrowserHTML, PreCrumbTemplateID, PreSurveyID, PreSurveyRatingMax, PreSurveyRatingMin, PreSurveyComment, Thumbnail, TurnDownCode, [Name], SurveyCategory
		from FromCrumbTemplate where crumbtype in (16,17,18,30,31) and fkStoreID=@StoreIDFrom and fkuserid =0'
	
	SET @CrumbTemplateSQL = REPLACE(@CrumbTemplateSQL, 'ToCrumbTemplate', @ToServerTablePrefix + 'CrumbTemplate')
	SET @CrumbTemplateSQL = REPLACE(@CrumbTemplateSQL, 'FromCrumbTemplate', @FromServerTablePrefix + 'CrumbTemplate')
	SET @CrumbTemplateSQL = REPLACE(@CrumbTemplateSQL, '@StoreIDTo', @StoreIDTo)
	SET @CrumbTemplateSQL = REPLACE(@CrumbTemplateSQL, '@StoreIDFrom', @StoreIDFrom)
	--SELECT @CrumbTemplateSQL
	--------------------------
	--  EXEC / INSERT CrumbTemplates
	--------------------------
	EXEC (@CrumbTemplateSQL)
	--------------------------
	--  Copy StoreCustomSource
	--------------------------
	DECLARE	@StoreCustomSourceSQL VARCHAR(MAX) =
	'	insert into ToStoreCustomSource (fkStoreID, SourceType, SourceDescription, Cost, DateCreated, DateModified, IsDeleted, CustomSource, DateStart, DateEnd, GUID, IsManual)
		select @StoreIDTo, SourceType, SourceDescription, [Cost], CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, NULL, NULL, NULL, NEWID(), IsManual
		from FromStoreCustomSource where isdeleted = 0 and fkStoreID=@StoreIDFrom'
	
	SET @StoreCustomSourceSQL = REPLACE(@StoreCustomSourceSQL, 'ToStoreCustomSource', @ToServerTablePrefix + 'StoreCustomSource')
	SET @StoreCustomSourceSQL = REPLACE(@StoreCustomSourceSQL, 'FromStoreCustomSource', @FromServerTablePrefix + 'StoreCustomSource')
	SET @StoreCustomSourceSQL = REPLACE(@StoreCustomSourceSQL, '@StoreIDTo', @StoreIDTo)
	SET @StoreCustomSourceSQL = REPLACE(@StoreCustomSourceSQL, '@StoreIDFrom', @StoreIDFrom)
	--SELECT @StoreCustomSourceSQL
	------------------------------------
	--  EXEC / INSERT StoreCustomSource
	------------------------------------
 	EXEC (@StoreCustomSourceSQL)
	-------------------------
	--	UPDATE Biz Rules
	-------------------------
	DECLARE		@BizRuleUpdateSQL VARCHAR(MAX) = 
	'
		UPDATE ToBusinessRule
		set fkCrumbTemplateID = 
		isnull((select top 1 pkcrumbtemplateid from ToCrumbTemplate 
				where	[Name] = (select [name] from FromCrumbTemplate where pkCrumbTemplateID = br2.fkCrumbTemplateID) and
						cast([Message] as nvarchar(max)) = (select cast([Message] as nvarchar(max)) from FromCrumbTemplate where pkCrumbTemplateID = br2.fkCrumbTemplateID) and
						fkStoreID=@StoreIDTo
			), 0)
		from ToBusinessRule br 
			outer apply (select * From Frombusinessrule br2 where br2.[name] = br.[name] and br2.delayminutes=br.delayminutes and br2.triggertype=br.triggertype and br2.fkstoreid=@StoreIDFrom) br2
		where br.fkStoreID=@StoreIDTo and br.eventtype <> 5000 and br.triggertype < 4900 
	'
	
	SET @BizRuleUpdateSQL = REPLACE(@BizRuleUpdateSQL, 'ToCrumbTemplate', @ToServerTablePrefix + 'CrumbTemplate')
	SET @BizRuleUpdateSQL = REPLACE(@BizRuleUpdateSQL, 'FromCrumbTemplate', @FromServerTablePrefix + 'CrumbTemplate')
	SET @BizRuleUpdateSQL = REPLACE(@BizRuleUpdateSQL, '@StoreIDTo', @StoreIDTo)
	SET @BizRuleUpdateSQL = REPLACE(@BizRuleUpdateSQL, '@StoreIDFrom', @StoreIDFrom)
	SET @BizRuleUpdateSQL = REPLACE(@BizRuleUpdateSQL, 'ToBusinessRule', @ToServerTablePrefix + 'BusinessRule')
	SET @BizRuleUpdateSQL = REPLACE(@BizRuleUpdateSQL, 'FromBusinessRule', @FromServerTablePrefix + 'BusinessRule')
	--SELECT	@BizRuleUpdateSQL 	
	------------------------------------
	--  EXEC / UPDATE Biz Rules
	------------------------------------
	EXEC (@BizRuleUpdateSQL)
-- if caddy then do part that used to be in Galaxy.dbo.CaddyMigrationFromPreviousVersion stored proc
IF (@HasFeatureCaddy = 1)   -- @HasFeatureCaddy bit = 1
BEGIN
/* doesn't work anymore
	EXEC [galaxy].Galaxy.dbo.CaddyMigrationFromPreviousVersion
		@FromStoreID = 38,  --Midnight Drive's template
		@ToStoreID = @StoreID,  --New Store
 
		@ToCaddyUserID    = @CaddyUserID,  --New Caddy user
		@FromCaddyUserID = 7852  --Midnight Drive's Caddy
*/

	DECLARE	@ToCaddyUserID	INT, @FromCaddyUserID	INT

	SET @FromStoreID = 38  --Midnight Drive's template
	SET @ToStoreID = @StoreID  --New Store
	SET @ToCaddyUserID = @CaddyUserID
	SET @FromCaddyUserID = 7852  --Midnight Drive's Caddy

	SELECT	@FromServerIP	 = (select TOP 1 ds.InternalServerIP from	[galaxy].Galaxy.dbo.DriveServer ds	INNER JOIN [galaxy].Galaxy.dbo.StoreDriveServerLink sds ON ds.pkDriveServerID = sds.fkDriveServerID AND sds.fkStoreID = @FromStoreID)
	SELECT	@FromServerName	 = (select TOP 1 ds.DataBaseName from	[galaxy].Galaxy.dbo.DriveServer ds	INNER JOIN [galaxy].Galaxy.dbo.StoreDriveServerLink sds ON ds.pkDriveServerID = sds.fkDriveServerID AND sds.fkStoreID = @FromStoreID)

	SELECT	@ToServerIP		 = (select TOP 1 ds.InternalServerIP from	[galaxy].Galaxy.dbo.DriveServer ds	INNER JOIN [galaxy].Galaxy.dbo.StoreDriveServerLink sds ON ds.pkDriveServerID = sds.fkDriveServerID AND sds.fkStoreID = @ToStoreID)
	SELECT	@ToServerName	 = (select TOP 1 ds.DataBaseName from	[galaxy].Galaxy.dbo.DriveServer ds	INNER JOIN [galaxy].Galaxy.dbo.StoreDriveServerLink sds ON ds.pkDriveServerID = sds.fkDriveServerID AND sds.fkStoreID = @ToStoreID)

	SELECT	@FromServerTablePrefix  = '[' + @FromServerIP + '].' + @FromServerName + '.dbo.'
	SELECT	@ToServerTablePrefix  = '[' + @ToServerIP + '].' + @ToServerName + '.dbo.'
	
	SELECT @StoreIDTo  = @ToStoreID
	SELECT @StoreIDFrom  = @FromStoreID 


	-- leave these alone
	DECLARE @CaddyIDTo		INT	= 0
	DECLARE @CaddyIDFrom	INT = 0
	
	SET	@CaddyIDTo = @ToCaddyUserID
	SET @CaddyIDFrom = @FromCaddyUserID

	
	-- copy crumbtemplates
	DECLARE @CaddyCrumbTemplateSQL VARCHAR(MAX) = 
	'INSERT INTO ToCrumbTemplate
	(
		fkStoreID,
		CrumbType,
		[Subject],
		[Message],
		DateCreated,
		DateModified,
		fkAttachedSurveyID,
		RuleIneligibleOpCode,
		RuleMileageMin,
		RuleMileageMax,
		BrowserHTML,
		PreCrumbTemplateID,
		PreSurveyID,
		PreSurveyRatingMax,
		PreSurveyRatingMin,
		PreSurveyComment,
		Thumbnail,
		TurnDownCode,
		Name,
		fkUserID,
		SurveyCategory
	)
	SELECT	@StoreIDTo,
			CrumbType,
			[Subject],
			[Message],
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP,
			fkAttachedSurveyID,
			RuleIneligibleOpCode,
			RuleMileageMin,
			RuleMileageMax,
			BrowserHTML,
			PreCrumbTemplateID,
			PreSurveyID,
			PreSurveyRatingMax,
			PreSurveyRatingMin,
			PreSurveyComment,
			Thumbnail,
			TurnDownCode,
			Name,
			@CaddyIDTo,
			SurveyCategory
	FROM	FromCrumbTemplate 
	WHERE	fkUserID = @CaddyIDFrom or (fkStoreID = @StoreIDFrom AND CrumbType = 2)'

	SET @CaddyCrumbTemplateSQL = REPLACE(@CaddyCrumbTemplateSQL, '@StoreIDTo', @StoreIDTo)
	SET @CaddyCrumbTemplateSQL = REPLACE(@CaddyCrumbTemplateSQL, '@StoreIDFrom', @StoreIDFrom)

	SET @CaddyCrumbTemplateSQL = REPLACE(@CaddyCrumbTemplateSQL, 'ToCrumbTemplate', @ToServerTablePrefix + 'CrumbTemplate')
	SET @CaddyCrumbTemplateSQL = REPLACE(@CaddyCrumbTemplateSQL, 'FromCrumbTemplate', @FromServerTablePrefix + 'CrumbTemplate')

	SET @CaddyCrumbTemplateSQL = REPLACE(@CaddyCrumbTemplateSQL, '@CaddyIDTo', @CaddyIDTo)
	SET @CaddyCrumbTemplateSQL = REPLACE(@CaddyCrumbTemplateSQL, '@CaddyIDFrom', @CaddyIDFrom)


	--SELECT @CaddyCrumbTemplateSQL
	EXEC (@CaddyCrumbTemplateSQL)


	-- copy biz rules and set crumb template id
	DECLARE @BizRulesSQL	VARCHAR(MAX) =
	'INSERT	INTO ToBusinessRule
	 (fkStoreID, Name, Description, TriggerType,EventType,     
			DelayMinutes,     
			TaskDueMinutes,     
			IsActive,     
			DateCreated,     
			DateModified,     
			IsDeleted,     
			fkCrumbTemplateID,     
			NewUsedType,     
			TimeOfDay,     
			IsIgnoredIfApptSetForFuture,     
			IsIgnoredIfApptShownInPast,     
			IsIgnoredIfCustomerSentEmail,     
			IsIgnoredIfDealIsSold,     
			IsIgnoredIfDealIsDead,    
			IsIgnoredIfDealIsDelivered,     
			IsIgnoredIfPhoneConversation,
			IsIgnoredIfNoVehicleInformation,
			AssignmentType,     
			MileageStart,
			MileageEnd,
			IneligibleOpCode,
			TurnDownCode,
			IsFirstSurvey,     
			IsGoodLastSurveyResponse,
			IsBadLastSurveyResponse,
			IsNoneLastSurveyResponse,     
			IsAllowedToBeCancelled,
			IsAllowedToRunOutsideBusinessHours,     
			fkSurveyID,
			IsIgnoredIfUserSentManualEmail,
			DealSourceSubType,     
			DealSourceType,
			MinimumFollowUp,
			ExecutionType,     
			fkCustomReportID,
			ChangeToCaddyStage,     
			IsIgnoredIfCustomerHasValidPhoneNumber,     
			IsIgnoredIfCustomerHasNoPhoneNumber,     
			CaddyHiringTriggerType,
			WhenToRunType,     
			IsIgnoredIfPhoneCallMade,
			IsIgnoredIfUserSentText,
			IsIgnoredIfCustomerTextedBack,     
			IsIgnoredIfCustomerHasNoEmail,
			IsIgnoredIfCustomerHasEmail,     
			TouchPointActionTypes)
	SELECT	@StoreIDTo,
			br.Name,     
			br.[Description],     
			TriggerType,     
			EventType,     
			DelayMinutes,     
			TaskDueMinutes,     
			IsActive,     
			CURRENT_TIMESTAMP,     
			CURRENT_TIMESTAMP,     
			br.IsDeleted,     
			ISNULL(cr2.pkCrumbTemplateID, 0),     
			NewUsedType,     
			TimeOfDay,     
			IsIgnoredIfApptSetForFuture,     
			IsIgnoredIfApptShownInPast,     
			IsIgnoredIfCustomerSentEmail,     
			IsIgnoredIfDealIsSold,     
			IsIgnoredIfDealIsDead,    
			IsIgnoredIfDealIsDelivered,     
			IsIgnoredIfPhoneConversation,
			IsIgnoredIfNoVehicleInformation,
			AssignmentType,     
			MileageStart,
			MileageEnd,
			IneligibleOpCode,
			br.TurnDownCode,
			IsFirstSurvey,     
			IsGoodLastSurveyResponse,
			IsBadLastSurveyResponse,
			IsNoneLastSurveyResponse,     
			IsAllowedToBeCancelled,
			IsAllowedToRunOutsideBusinessHours,     
			fkSurveyID,
			IsIgnoredIfUserSentManualEmail,
			DealSourceSubType,     
			DealSourceType,
			MinimumFollowUp,
			ExecutionType,     
			fkCustomReportID,
			ChangeToCaddyStage,     
			IsIgnoredIfCustomerHasValidPhoneNumber,     
			IsIgnoredIfCustomerHasNoPhoneNumber,     
			CaddyHiringTriggerType,
			WhenToRunType,     
			IsIgnoredIfPhoneCallMade,
			IsIgnoredIfUserSentText,
			IsIgnoredIfCustomerTextedBack,     
			IsIgnoredIfCustomerHasNoEmail,
			IsIgnoredIfCustomerHasEmail,     
			'''' AS TouchPointActionTypes
	FROM	FromBusinessRule br
			LEFT JOIN  FromCrumbTemplate cr1 on cr1.pkCrumbTemplateID=br.fkCrumbTemplateID and br.fkCrumbTemplateID > 0
			OUTER APPLY
			(SELECT  TOP 1 *  
			 FROM	ToCrumbTemplate cr2 
			 WHERE  cr2.fkStoreID=@StoreIDTo 
					AND convert(varchar(max), cr1.[Message]) = convert(varchar(max), cr2.[Message]) 
					and cr1.[Subject] = cr2.[Subject]
			) cr2
	WHERE	br.fkStoreID = @StoreIDFrom 
			AND 
			(
				(
					TriggerType >= 4900 
				)
				 
				OR eventtype = 5000 
			) 
			AND br.IsDeleted = 0 
			AND br.isActive  = 1'

	--and br.IsActive=1 

	SET @BizRulesSQL = REPLACE(@BizRulesSQL, '@StoreIDTo', @StoreIDTo)
	SET @BizRulesSQL = REPLACE(@BizRulesSQL, '@StoreIDFrom', @StoreIDFrom)

	SET @BizRulesSQL = REPLACE(@BizRulesSQL, 'ToCrumbTemplate', @ToServerTablePrefix + 'CrumbTemplate')
	SET @BizRulesSQL = REPLACE(@BizRulesSQL, 'FromCrumbTemplate', @FromServerTablePrefix + 'CrumbTemplate')

	SET @BizRulesSQL = REPLACE(@BizRulesSQL, 'ToBusinessRule', @ToServerTablePrefix + 'BusinessRule')
	SET @BizRulesSQL = REPLACE(@BizRulesSQL, 'FromBusinessRule', @FromServerTablePrefix + 'BusinessRule')

	SET @BizRulesSQL = REPLACE(@BizRulesSQL, '@CaddyIDTo', @CaddyIDTo)
	SET @BizRulesSQL = REPLACE(@BizRulesSQL, '@CaddyIDFrom', @CaddyIDFrom)



	--SELECT @BizRulesSQL
	EXEC (@BizRulesSQL)


	-- copy biz rule users
	DECLARE @BizRulesUser	VARCHAR(MAX) =
	'	INSERT INTO ToBusinessRuleUser
		(
			fkBusinessRuleID,
			UserType,
			fkUserID,
			DateCreated,
			DateModified,
			IsDeleted,
			fkDistributionID
		)	
		SELECT	pkBusinessRuleID, 100, @CaddyIDTo, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, 0
		FROM	ToBusinessRule br 
		WHERE	br.TriggerType >= 4900 AND br.TriggerType < 6000 and br.fkStoreID=@StoreIDTo'


	SET @BizRulesUser = REPLACE(@BizRulesUser, '@StoreIDTo', @StoreIDTo)
	SET @BizRulesUser = REPLACE(@BizRulesUser, '@StoreIDFrom', @StoreIDFrom)

	SET @BizRulesUser = REPLACE(@BizRulesUser, 'ToBusinessRule', @ToServerTablePrefix + 'BusinessRule')
	SET @BizRulesUser = REPLACE(@BizRulesUser, 'FromBusinessRule', @FromServerTablePrefix + 'BusinessRule')


	SET @BizRulesUser = REPLACE(@BizRulesUser, '@CaddyIDTo', @CaddyIDTo)
	SET @BizRulesUser = REPLACE(@BizRulesUser, '@CaddyIDFrom', @CaddyIDFrom)


	
	EXEC (@BizRulesUser)


END


-- set biz rules inactive
EXEC ('UPDATE [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.BusinessRule SET IsActive = 0 WHERE fkStoreID = ' + @StoreIDString)

-- make sure caddy templates are not duplicated
EXEC ('delete ct
	from [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.CrumbTemplate ct
		left join [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.BusinessRule br on br.fkCrumbTemplateID=ct.pkCrumbTemplateID
	where br.pkBusinessRuleID is null
		and ct.Name like ''%caddy%''')


--Set default Video Template in StoreAttribute equal to 'I just recorded a video ... ' template from store's CrumbTemplate table
DROP TABLE IF EXISTS #CTID
CREATE TABLE #CTID (id int)

DECLARE @CrumbTemplateIDSQL NVARCHAR(MAX) = 
	'INSERT INTO #CTID
		SELECT pkCrumbTemplateID as id
	FROM [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.CrumbTemplate ct
	where ct.Subject = ''RE: I just recorded a video for you'' and fkStoreID = ' + @StoreIDString 

DECLARE @CrumbTemplateID int = 0
EXECUTE sp_executesql @CrumbTemplateIDSQL, N'@CrumbTemplateID INTEGER OUTPUT', @CrumbTemplateID OUTPUT

DECLARE @CTID int = 0
SELECT @CTID = id FROM #CTID



DECLARE @StoreAttributeSQL VARCHAR(MAX) =
	'INSERT INTO [galaxy].Galaxy.dbo.StoreAttribute ( 
		fkStoreID ,
		[Key] ,
		Value ,
		DateCreated ,
		DateModified ,
		IsDeleted)
	VALUES  ( 
		@StoreIDString,
		''DefaultVideoTemplate'',
		@CTID,
		CURRENT_TIMESTAMP,
		CURRENT_TIMESTAMP,
		0)'


SET @StoreAttributeSQL = REPLACE(@StoreAttributeSQL, '@StoreIDString', @StoreIDString)
SET @StoreAttributeSQL = REPLACE(@StoreAttributeSQL, '@CTID', @CTID)


--SELECT @StoreAttributeSQL
	EXEC (@StoreAttributeSQL)


--was previously in MergeDMS, moved to store creation
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StorePreference ( 
			fkStoreID ,
			GUID,
			PreferenceType,
			PreferenceValue,
			PreferenceOptions,
			DateCreated,
			DateModified ,
			IsDeleted,
			ModifiedDateViaTrigger)
	VALUES (
			' + @StoreIDString + ',
			NEWID(),
			''UserSessionLength'',
			''60'',
			'''',
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP,
			0,
			CURRENT_TIMESTAMP)'
	)

	--ON-2941 - change default behavior for 'text opt-in required' for all stores
	EXEC('INSERT INTO [' + @DatabaseIP + '].' + @DatabaseName + '.dbo.StorePreference ( 
			fkStoreID ,
			GUID,
			PreferenceType,
			PreferenceValue,
			PreferenceOptions,
			DateCreated,
			DateModified ,
			IsDeleted,
			ModifiedDateViaTrigger)
	VALUES (
			' + @StoreIDString + ',
			NEWID(),
			''TextOptInRequired'',
			''true'',
			'''',
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP,
			0,
			CURRENT_TIMESTAMP)'
	)
