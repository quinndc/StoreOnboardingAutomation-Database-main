
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [dbo].[CRMExtraction]
    @StoreID VARCHAR(5),
	@StartDate DATETIME,
	@EndDate DATETIME,
	@folder_location VARCHAR(300) OUTPUT
AS
BEGIN

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..##appointment') IS NOT NULL
    DROP TABLE ##appointment

IF OBJECT_ID('tempdb..##contacts') IS NOT NULL
    DROP TABLE ##contacts

IF OBJECT_ID('tempdb..##customerNotes') IS NOT NULL
    DROP TABLE ##customerNotes

IF OBJECT_ID('tempdb..##customers') IS NOT NULL
    DROP TABLE ##customers

IF OBJECT_ID('tempdb..##dealNotes') IS NOT NULL
    DROP TABLE ##dealNotes

IF OBJECT_ID('tempdb..##deals') IS NOT NULL
    DROP TABLE ##deals

IF OBJECT_ID('tempdb..##messageDetails') IS NOT NULL
    DROP TABLE ##messageDetails

IF OBJECT_ID('tempdb..##customerOptOut') IS NOT NULL
    DROP TABLE ##customerOptOut

IF OBJECT_ID('tempdb..##taskDetails') IS NOT NULL
    DROP TABLE ##taskDetails

IF OBJECT_ID('tempdb..##users') IS NOT NULL
    DROP TABLE ##users


    DECLARE @sql NVARCHAR(MAX)
    DECLARE @IP VARCHAR(1024)
    DECLARE @leadcrumb VARCHAR(1024)
    DECLARE @bcpCommand NVARCHAR(4000)
	DECLARE @date VARCHAR(20)
    SET @date = CONVERT(VARCHAR(20), GETDATE(), 112)


        CREATE TABLE #temp (
            InternalServerIP VARCHAR(1024),
            DatabaseName VARCHAR(1024)
        )

        INSERT INTO #temp (InternalServerIP, DatabaseName)
        SELECT 
            ds.InternalServerIP,
            ds.DatabaseName
        FROM Galaxy.Galaxy.dbo.Store s
        LEFT JOIN Galaxy.Galaxy.dbo.StoreDriveServerLink sdsl ON s.pkStoreID = sdsl.fkStoreID
        LEFT JOIN Galaxy.Galaxy.dbo.DriveServer ds ON sdsl.fkDriveServerID = ds.pkDriveServerID
        LEFT JOIN Galaxy.[DBAAdmin].[dbo].[_ServerList] sl ON ds.InternalServerIP = CAST(sl.ip_address AS VARCHAR(MAX))
        WHERE s.isDeleted = 0
        AND s.pkStoreID = @StoreID

        SELECT TOP 1 @IP = InternalServerIP, @leadcrumb = DatabaseName FROM #temp

        DROP TABLE #temp
--------------------------------------
--Appointment			   
--------------------------------------
CREATE TABLE ##appointment (
    AppointmentID VARCHAR(1024),
    CustomerId VARCHAR(1024),
    DealId VARCHAR(1024),
    UserId VARCHAR(1024),
    AppointmentDateCreated VARCHAR(1024),
    AppointmentDate VARCHAR(1024),
    AppointmentDateCompleted VARCHAR(1024),
    AppointmentDateConfirmed VARCHAR(1024),
    AppointmentDateCancelled VARCHAR(1024),
    AppointmentTypeName VARCHAR(1024)
)
        SET @sql = N'
		INSERT INTO ##appointment (AppointmentID, CustomerId, DealId, UserId, AppointmentDateCreated, AppointmentDate, AppointmentDateCompleted, AppointmentDateConfirmed, AppointmentDateCancelled, AppointmentTypeName)
        SELECT 
		a.[GUID] AS AppointmentID,
		c.[GUID] AS CustomerId,
		d.[GUID] AS DealId,
		u.[GUID] AS UserId,
		a.DateCreated AS AppointmentDateCreated,
		a.DateStart AS AppointmentDate,
		a.DateCompleted  AS AppointmentDateCompleted,
		a.DateConfirmed  AS AppointmentDateConfirmed,
		a.DateCancelled AS AppointmentDateCancelled,
		CASE AppointmentType
			WHEN 1 THEN ''Sales''
			WHEN 2 THEN ''Delivery''
			WHEN 3 THEN ''Service''
			WHEN 4 THEN ''General''
			WHEN 5 THEN ''Test Drive''
			ELSE ''Undefined''
		END AS AppointmentTypeName
FROM	[' + @IP + '].[' + @leadcrumb + '].[dbo].[Appointment] a (NOLOCK)
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].[dbo].[Customer] c (NOLOCK) ON c.pkCustomerID = a.fkCustomerID AND c.IsDeleted = 0
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].[dbo].[User] u (NOLOCK) ON a.fkCreatedByUserID = u.pkUserID
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].[dbo].Deal d (NOLOCK) ON d.pkDealID = a.fkDealID
WHERE	a.fkStoreID = @StoreID
		AND a.DateCreated BETWEEN @StartDate AND @EndDate'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare Folder Path and File Path
DECLARE @folderPath VARCHAR(300) = 'C:\Exports\' + @StoreID +'_' + @leadcrumb + '_' + @date 
set @folder_location = @folderPath
DECLARE @appointmentPath VARCHAR(300) = @folderPath + '\appointment.csv'
DECLARE @mkdirCommand NVARCHAR(4000)

-- Prepare the query for bcp
DECLARE @query NVARCHAR(4000)
SET @query = '
	SELECT AppointmentID, 
		CustomerId, 
		DealId, 
		UserId, 
		AppointmentDateCreated, 
		AppointmentDate, 
		AppointmentDateCompleted, 
		AppointmentDateConfirmed, 
		AppointmentDateCancelled, 
		AppointmentTypeName 
		FROM (
			SELECT ''AppointmentID'' AS AppointmentID, 
				''CustomerId'' AS CustomerId, 
				''DealId'' AS DealId, 
				''UserId'' AS UserId, 
				''AppointmentDateCreated'' AS AppointmentDateCreated, 
				''AppointmentDate'' AS AppointmentDate, 
				''AppointmentDateCompleted'' AS AppointmentDateCompleted, 
				''AppointmentDateConfirmed'' AS AppointmentDateConfirmed, 
				''AppointmentDateCancelled'' AS AppointmentDateCancelled, 
				''AppointmentTypeName'' AS AppointmentTypeName 
				UNION ALL 
			SELECT CONVERT(VARCHAR(1024), AppointmentID), 
				CONVERT(VARCHAR(1024), CustomerId), 
				CONVERT(VARCHAR(1024), DealId), 
				CONVERT(VARCHAR(1024), UserId), 
				CONVERT(VARCHAR(1024), 
				ISNULL(AppointmentDateCreated, '''')), 
				CONVERT(VARCHAR(1024), 
				ISNULL(AppointmentDate, '''')), 
				CONVERT(VARCHAR(1024), 
				ISNULL(AppointmentDateCompleted, '''')), 
				CONVERT(VARCHAR(1024), 
				ISNULL(AppointmentDateConfirmed, '''')), 
				CONVERT(VARCHAR(1024), 
				ISNULL(AppointmentDateCancelled, '''')), 
				CONVERT(VARCHAR(1024), AppointmentTypeName) 
				FROM ##appointment) 
			AS CombinedResults'

--Create Folder
SET @mkdirCommand = 'IF NOT EXIST "' + @folderPath + '" mkdir "' + @folderPath + '"'
EXEC xp_cmdshell @mkdirCommand, no_output

SET @bcpCommand = 'bcp "' + @query + '" queryout "' + @appointmentPath + '" -c -t, -T -S ' + @@SERVERNAME 
EXEC xp_cmdshell @bcpCommand

-- Cleanup
DROP TABLE ##appointment
--------------------------------------
--Contacts			   
--------------------------------------
CREATE TABLE ##contacts (
    CustomerId VARCHAR(100),
    [Type] VARCHAR(100),
    Label VARCHAR(100),
    [Value] VARCHAR(100),
    IsPreferred BIT
)


SET @sql = N'
	INSERT INTO ##contacts (CustomerId, [Type], Label, [Value], IsPreferred)
    SELECT 
        c.[GUID] AS CustomerId,
        CASE
            WHEN cc.CommunicationType = 1 THEN ''Phone''
            WHEN cc.CommunicationType = 2 THEN ''Email''
        END AS [Type],
        CASE
            WHEN cc.ContactLabelType = 1 THEN ''Home''
            WHEN cc.ContactLabelType = 2 THEN ''Mobile''
            WHEN cc.ContactLabelType = 3 THEN ''Work''
        END AS Label,
         LEFT(LOWER(cc.[Value]), 100) as [Value],
        cc.IsPreferred
    FROM    [' + @IP + '].[' + @leadcrumb + '].dbo.[CustomerContact] cc (NOLOCK)
            INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[Customer] c (NOLOCK) ON c.pkCustomerID = cc.fkCustomerID AND c.IsDeleted = 0 
    WHERE   cc.IsDeleted = 0 
            AND cc.IsBad = 0
            AND c.fkStoreID = @StoreID
            AND cc.DateCreated BETWEEN @StartDate AND @EndDate'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @contactsPath VARCHAR(255) = @folderPath + '\contacts.csv'

-- Prepare the query for bcp
DECLARE @contactsQuery NVARCHAR(4000)
SET @contactsQuery = 'SELECT CustomerId, [Type], Label, [Value], IsPreferred FROM (SELECT ''CustomerId'' AS CustomerId, ''Type'' AS [Type], ''Label'' AS Label, ''Value'' AS [Value], ''IsPreferred'' AS IsPreferred UNION ALL SELECT CONVERT(VARCHAR(1024), CustomerId), CONVERT(VARCHAR(1024), [Type]), CONVERT(VARCHAR(1024), Label), CONVERT(VARCHAR(1000), [Value]), CONVERT(VARCHAR(1024), IsPreferred) FROM ##contacts) AS CombinedContacts'

--Create CSV
SET @bcpCommand = 'bcp "' + @contactsQuery + '" queryout "' + @contactsPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand

DROP TABLE ##contacts


--------------------------------------
--Customer Notes		   
--------------------------------------
CREATE TABLE ##customerNotes (
    CustomerId VARCHAR(1024),
    UserId VARCHAR(1024),
    Notes VARCHAR(8000), 
    DateCreated VARCHAR(100)
)

SET @sql = N'
	INSERT INTO ##customerNotes (CustomerId, UserId, Notes, DateCreated)
SELECT		
		c.[GUID] AS CustomerId,		
		u.[GUID] AS UserId,
		LEFT(dbo.fnParseOnlyAlphaWithSpaces(cl.Notes), 8000) AS [Notes],
		cl.DateCreated
FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.CustomerLog cl
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[Customer] c (NOLOCK) ON c.pkCustomerID = cl.fkCustomerID AND c.IsDeleted = 0
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.Deal d (NOLOCK) ON d.pkDealID = cl.fkDealID AND d.IsDeleted = 0
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u (NOLOCK) ON u.pkUserID = cl.fkUserID
WHERE	cl.ContactType IN (1,8)
		AND c.fkStoreID = @StoreID
		AND cl.DateCreated BETWEEN @StartDate AND @EndDate'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @customernotesPath VARCHAR(255) = @folderPath + '\customernotes.csv'

-- Prepare the query for bcp
DECLARE @customernotesQuery NVARCHAR(4000)
SET @customernotesQuery = 'SELECT CustomerId, UserId, Notes, DateCreated FROM (SELECT ''CustomerId'' AS CustomerId, ''UserId'' AS UserId, ''Notes'' AS Notes, ''DateCreated'' AS DateCreated UNION ALL SELECT CONVERT(VARCHAR(1024), CustomerId), CONVERT(VARCHAR(1024), UserId), CONVERT(VARCHAR(8000), Notes), CONVERT(VARCHAR(1024), DateCreated) FROM ##customerNotes) AS CombinedCustomerNotes'

--Create CSV
SET @bcpCommand = 'bcp "' + @customernotesQuery + '" queryout "' + @customernotesPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand


DROP TABLE ##customerNotes


--------------------------------------
--Customers		
--------------------------------------
CREATE TABLE ##customers (
    CustomerId VARCHAR(1024),
    [Type] VARCHAR(1024),
    FirstName VARCHAR(1024),
    LastName VARCHAR(1024),
    CompanyName VARCHAR(100),
    Birthday VARCHAR(100),
    UserSales1Id VARCHAR(1024),
    UserSales2Id VARCHAR(1024),
    UserBDCId VARCHAR(1024),
    Address1 VARCHAR(100),
    Address2 VARCHAR(100),
    City VARCHAR(1024),
    [State] VARCHAR(1024),
    [Zip] VARCHAR(10),
    DateCreated VARCHAR(1024)
)


SET @sql = N'
INSERT INTO ##customers (CustomerId, [Type], FirstName, LastName, CompanyName, Birthday, UserSales1Id, UserSales2Id, UserBDCId, Address1, Address2, City, [State], [Zip], DateCreated)
SELECT	c.[GUID] AS CustomerId,
		CASE
			WHEN CustomerType = 10 THEN ''Company''
			ELSE ''Individual''
		END AS [Type],
		PrimaryFirstName AS FirstName,
		PrimaryLastName	 AS LastName,
		CompanyName as CompanyName,
		CAST(NULLIF(PrimaryDOB, ''1-1-1900'') AS DATE) AS Birthday,
		u.[GUID] AS UserSales1Id,
		u2.[GUID] AS UserSales2Id,
		u3.[GUID] AS UserBDCId,
		dbo.fnParseOnlyAlphaWithSpaces(Address1) AS Address1, 
		dbo.fnParseOnlyAlphaWithSpaces(Address2) AS Address2,
		dbo.fnParseOnlyAlphaWithSpaces(City) AS City,
		dbo.fnParseOnlyAlphaWithSpaces([State]) AS [State],
		dbo.fnParseOnlyAlphaWithSpaces([Zip]) AS [Zip],
		c.DateCreated AS DateCreated
FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.Customer c (NOLOCK)
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u (NOLOCK) ON c.fkUserIDSales1 = u.pkUserID
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u2 (NOLOCK) ON c.fkUserIDSales2 = u2.pkUserID
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u3 (NOLOCK) ON c.fkUserIDBDC = u3.pkUserID
WHERE	c.IsDeleted = 0		
		AND c.fkStoreID = @StoreID
		AND c.DateCreated BETWEEN @StartDate AND @EndDate'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @customersPath VARCHAR(255) = @folderPath + '\customers.csv'


-- Prepare the query for bcp
DECLARE @customersQuery NVARCHAR(4000)
SET @customersQuery = 'SELECT CustomerId, [Type], FirstName, LastName, CompanyName, Birthday, UserSales1Id, UserSales2Id, UserBDCId, Address1, Address2, City, [State], [Zip], DateCreated FROM (SELECT ''CustomerId'' AS CustomerId, ''[Type]'' AS [Type], ''FirstName'' AS FirstName, ''LastName'' AS LastName, ''CompanyName'' AS CompanyName, ''Birthday'' AS Birthday, ''UserSales1Id'' AS UserSales1Id, ''UserSales2Id'' AS UserSales2Id, ''UserBDCId'' AS UserBDCId, ''Address1'' AS Address1, ''Address2'' AS Address2, ''City'' AS City, ''[State]'' AS [State], ''[Zip]'' AS [Zip], ''DateCreated'' AS DateCreated UNION ALL SELECT CONVERT(VARCHAR(1024), CustomerId), CONVERT(VARCHAR(1024), [Type]), CONVERT(VARCHAR(1024), FirstName), CONVERT(VARCHAR(1024), LastName), CONVERT(VARCHAR(100), CompanyName), CONVERT(VARCHAR(1024), Birthday), CONVERT(VARCHAR(1024), UserSales1Id), CONVERT(VARCHAR(1024), UserSales2Id), CONVERT(VARCHAR(1024), UserBDCId), CONVERT(VARCHAR(100), Address1), CONVERT(VARCHAR(100), Address2), CONVERT(VARCHAR(1024), City), CONVERT(VARCHAR(1024), [State]), CONVERT(VARCHAR(10), [Zip]), CONVERT(VARCHAR(1024), DateCreated) FROM ##customers) AS CombinedCustomers'


--Create CSV
SET @bcpCommand = 'bcp "' + @customersQuery + '" queryout "' + @customersPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand

DROP TABLE ##customers

--------------------------------------
--Deal Notes		   
--------------------------------------
CREATE TABLE ##dealNotes (
    DealID VARCHAR(1024),
    UserId VARCHAR(1024),
    Message VARCHAR(8000),
    DateCreated VARCHAR(100)
)

SET @sql = N'
INSERT INTO ##dealNotes (DealID, UserId, Message, DateCreated)
SELECT		
		d.[GUID] AS DealID,		
		u.[GUID] AS UserId,
		dbo.fnParseOnlyAlphaWithSpaces(dl.Message) AS [Message],
		dl.DateCreated
FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.DealLog dl
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.Deal d (NOLOCK) ON d.pkDealID = dl.fkDealID AND d.IsDeleted = 0
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u (NOLOCK) ON u.pkUserID = dl.fkUserID
WHERE	d.fkStoreID = @StoreID
		AND dl.DealLogType IN (251,242)
		AND dl.DateCreated BETWEEN @StartDate AND @EndDate

'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @dealnotesPath VARCHAR(255) = @folderPath + '\dealnotes.csv'

-- Prepare the query for bcp
DECLARE @dealnotesQuery NVARCHAR(4000)
SET @dealnotesQuery = 'SELECT DealID, UserId, Message, DateCreated FROM (SELECT ''DealID'' AS DealID, ''UserId'' AS UserId, ''Message'' AS Message, ''DateCreated'' AS DateCreated UNION ALL SELECT CONVERT(VARCHAR(1024), DealID), CONVERT(VARCHAR(1024), UserId), CONVERT(VARCHAR(8000), Message), CONVERT(VARCHAR(1024), DateCreated) FROM ##dealNotes) AS CombinedDealNotes'

--Create CSV
SET @bcpCommand = 'bcp "' + @dealnotesQuery + '" queryout "' + @dealnotesPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand

DROP TABLE ##dealNotes


----------------------------------------
----Deals			   
----------------------------------------

CREATE TABLE ##deals (
    DealId VARCHAR(1024),
    DealDateCreated VARCHAR(1024),
    SourceType VARCHAR(1024),
    SourceDescription VARCHAR(8000),
    PrimaryCustomerId VARCHAR(1024),
    CoCustomerId VARCHAR(1024),
    UserSales1Id VARCHAR(1024),
    UserSales2Id VARCHAR(1024),
    UserBDCId VARCHAR(1024),
    VehicleYear VARCHAR(1024),
    VehicleMake VARCHAR(1024),
    VehicleModel VARCHAR(1024),
    VehicleVIN VARCHAR(1024),
    VehicleStockNumber VARCHAR(1024),
    VehicleMileage VARCHAR(1024),
    VehicleNewUsed VARCHAR(1024),
    TradeYear VARCHAR(1024),
    TradeMake VARCHAR(1024),
    TradeModel VARCHAR(1024),
    TradeVIN VARCHAR(1024),
    TradeMileage VARCHAR(1024),
    TradeNewUsed VARCHAR(1024),
    DeliveryDate VARCHAR(1024),
    DeadDate VARCHAR(1024)
)


SET @sql = N'
INSERT INTO ##deals (DealId, DealDateCreated, SourceType, SourceDescription, PrimaryCustomerId, CoCustomerId, UserSales1Id, UserSales2Id, UserBDCId, VehicleYear, VehicleMake, VehicleModel, VehicleVIN, VehicleStockNumber, VehicleMileage, VehicleNewUsed, TradeYear, TradeMake, TradeModel, TradeVIN, TradeMileage, TradeNewUsed, DeliveryDate, DeadDate)
SELECT	d.[GUID] AS DealId,
		d.DateCreated AS DealDateCreated,
		CASE
			WHEN d.SourceType = 6 THEN ''Internet''
			WHEN d.SourceType = 3 THEN ''Phone''
			WHEN d.SourceType = 1 THEN ''Showroom''
			ELSE ''Other''
		END AS SourceType,
		dbo.fnParseOnlyAlphaWithSpaces(d.SourceDescription) AS SourceDescription,
		c.[GUID] AS PrimaryCustomerId,
		c2.[GUID] AS CoCustomerId,		
		u.[GUID] AS UserSales1Id,
		u2.[GUID] AS UserSales2Id,
		u3.[GUID] AS UserBDCId,
		interestVehicle.[Year] AS VehicleYear,
		dbo.fnParseOnlyAlphaWithSpaces(interestVehicle.Make) AS VehicleMake,
		dbo.fnParseOnlyAlphaWithSpaces(interestVehicle.Model) AS VehicleModel,
		dbo.fnParseOnlyAlphaWithSpaces(interestVehicle.VIN) AS VehicleVIN,
		dbo.fnParseOnlyAlphaWithSpaces(interestVehicle.StockNumber) AS VehicleStockNumber,
		interestVehicle.OdometerStatus AS VehicleMileage,
		CASE WHEN  interestVehicle.NewUsedType = 1 THEN ''New'' ELSE ''Used'' END AS VehicleNewUsed,
		tradeIn.[Year] AS TradeYear,
		dbo.fnParseOnlyAlphaWithSpaces(tradeIn.Make) AS TradeMake,
		dbo.fnParseOnlyAlphaWithSpaces(tradeIn.Model) AS TradeModel,
		dbo.fnParseOnlyAlphaWithSpaces(tradeIn.VIN) AS TradeVIN,
		tradeIn.OdometerStatus AS TradeMileage,
		''Used'' AS TradeNewUsed,
		dfDelivered.DateCreated AS DeliveryDate,
		dfDead.DateCreated AS DeadDate
FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.Deal d (NOLOCK)
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[Customer] c (NOLOCK) ON c.pkCustomerID = d.fkCustomerID AND c.IsDeleted = 0
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[Customer] c2 (NOLOCK) ON c2.pkCustomerID = d.fkCoBuyerCustomerID AND c.IsDeleted = 0
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.DealFlag dfDelivered (NOLOCK) ON dfDelivered.fkDealID = d.pkDealID AND dfDelivered.DealFlagType = 9
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.DealFlag dfDead (NOLOCK) ON dfDead.fkDealID = d.pkDealID AND dfDead.DealFlagType = 5
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u (NOLOCK) ON d.fkUserIDSales1 = u.pkUserID
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u2 (NOLOCK) ON d.fkUserIDSales2 = u2.pkUserID
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u3 (NOLOCK) ON d.fkILMUserID = u3.pkUserID
		OUTER APPLY
		(
			SELECT	TOP 1 * 
			FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.CustomerVehicle cv (NOLOCK) 
			WHERE	cv.fkDealID = d.pkDealID AND cv.InterestType <> 4

		) interestVehicle
		OUTER APPLY
		(
			SELECT	TOP 1 * 
			FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.CustomerVehicle cv (NOLOCK) 
			WHERE	cv.fkDealID = d.pkDealID AND cv.InterestType = 4
		) tradeIn
WHERE	d.IsDeleted = 0
		AND d.fkStoreID = @StoreID
		AND d.fkDuplicateDealID = 0
		AND d.DateCreated BETWEEN @StartDate AND @EndDate
'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @dealsPath VARCHAR(255) = @folderPath + '\deals.csv'

-- Prepare the query for bcp
DECLARE @dealsQuery NVARCHAR(4000)
SET @dealsQuery = 'SELECT DealId, DealDateCreated, SourceType, SourceDescription, PrimaryCustomerId, CoCustomerId, UserSales1Id, UserSales2Id, UserBDCId, VehicleYear, VehicleMake, VehicleModel, VehicleVIN, VehicleStockNumber, VehicleMileage, VehicleNewUsed, TradeYear, TradeMake, TradeModel, TradeVIN, TradeMileage, TradeNewUsed, DeliveryDate, DeadDate FROM (SELECT ''DealId'' AS DealId, ''DealDateCreated'' AS DealDateCreated, ''SourceType'' AS SourceType, ''SourceDescription'' AS SourceDescription, ''PrimaryCustomerId'' AS PrimaryCustomerId, ''CoCustomerId'' AS CoCustomerId, ''UserSales1Id'' AS UserSales1Id, ''UserSales2Id'' AS UserSales2Id, ''UserBDCId'' AS UserBDCId, ''VehicleYear'' AS VehicleYear, ''VehicleMake'' AS VehicleMake, ''VehicleModel'' AS VehicleModel, ''VehicleVIN'' AS VehicleVIN, ''VehicleStockNumber'' AS VehicleStockNumber, ''VehicleMileage'' AS VehicleMileage, ''VehicleNewUsed'' AS VehicleNewUsed, ''TradeYear'' AS TradeYear, ''TradeMake'' AS TradeMake, ''TradeModel'' AS TradeModel, ''TradeVIN'' AS TradeVIN, ''TradeMileage'' AS TradeMileage, ''TradeNewUsed'' AS TradeNewUsed, ''DeliveryDate'' AS DeliveryDate, ''DeadDate'' AS DeadDate UNION ALL SELECT CAST(DealId AS VARCHAR(1024)), CAST(DealDateCreated AS VARCHAR(1024)), CAST(SourceType AS VARCHAR(1024)), CAST(SourceDescription AS VARCHAR(8000)), CAST(PrimaryCustomerId AS VARCHAR(1024)), CAST(CoCustomerId AS VARCHAR(1024)), CAST(UserSales1Id AS VARCHAR(1024)), CAST(UserSales2Id AS VARCHAR(1024)), CAST(UserBDCId AS VARCHAR(1024)), CAST(VehicleYear AS VARCHAR(1024)), CAST(VehicleMake AS VARCHAR(1024)), CAST(VehicleModel AS VARCHAR(1024)), CAST(VehicleVIN AS VARCHAR(1024)), CAST(VehicleStockNumber AS VARCHAR(1024)), CAST(VehicleMileage AS VARCHAR(1024)), CAST(VehicleNewUsed AS VARCHAR(1024)), CAST(TradeYear AS VARCHAR(1024)), CAST(TradeMake AS VARCHAR(1024)), CAST(TradeModel AS VARCHAR(1024)), CAST(TradeVIN AS VARCHAR(1024)), CAST(TradeMileage AS VARCHAR(1024)), CAST(TradeNewUsed AS VARCHAR(1024)), CAST(DeliveryDate AS VARCHAR(1024)), CAST(DeadDate AS VARCHAR(1024)) FROM ##deals) AS CombinedDeals'

--Create CSV
SET @bcpCommand = 'bcp "' + @dealsQuery + '" queryout "' + @dealsPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand

DROP TABLE ##deals


----------------------------------------
----Messages		   
----------------------------------------

CREATE TABLE ##messageDetails (
    MessageId VARCHAR(1024),
    CustomerId VARCHAR(1024),
    DealId VARCHAR(100),
    UserId VARCHAR(100),
    [To] VARCHAR(255),
    [From] VARCHAR(255),
    MessageType VARCHAR(1024),
    [Message] VARCHAR(8000),
    DateCreated VARCHAR(1024)
)


SET @sql = N'
INSERT INTO ##messageDetails (MessageId, CustomerId, DealId, UserId, [To], [From], MessageType, [Message], DateCreated)
SELECT	cr.[Guid] AS MessageId,
		c.[GUID] AS CustomerId,
		d.[GUID] AS DealId,
		u.[GUID] AS UserId,
		cr.[To],
		CASE WHEN cr.[From] = ''CDYNESMS'' THEN ''CRM''  ELSE cr.[From] END AS [From],
		CASE 
			WHEN cr.CrumbType IN (2,36) THEN ''OutgoingText''
			WHEN cr.CrumbType IN (33,35) THEN ''IncomingText''
			WHEN cr.CrumbType IN (15) THEN ''IncomingEmail''
			WHEN cr.CrumbType IN (10) THEN ''ManualOutgoingEmail''
			--WHEN cr.CrumbType IN (19) THEN ''AutoOutgoingEmail''
		END AS MessageType,
		dbo.fnParseOnlyAlphaWithSpaces(cr.StrippedMessage) AS [Message],
		cr.DateCreated
FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.[Crumb] cr (NOLOCK)
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[Customer] c (NOLOCK) ON c.pkCustomerID = cr.fkReferenceID AND c.IsDeleted = 0
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u (NOLOCK) ON u.pkUserID = cr.fkUserID AND u.UserType <> 80
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.Deal d (NOLOCK) ON d.pkDealID = cr.fkDealID AND d.IsDeleted = 0
WHERE	cr.fkStoreID = @StoreID
		AND cr.CrumbType IN (2,10,15,33,35,36)
		AND cr.DateCreated BETWEEN @StartDate AND  @EndDate
'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @messagesPath VARCHAR(255) = @folderPath + '\messages.csv'

-- Prepare the query for bcp
DECLARE @messagesQuery NVARCHAR(4000)
SET @messagesQuery = 'SELECT MessageId, CustomerId, DealId, UserId, [To], [From], MessageType, [Message], DateCreated 
FROM (SELECT ''MessageId'' AS MessageId, ''CustomerId'' AS CustomerId, ''DealId'' AS DealId, ''UserId'' AS UserId, ''[To]'' AS [To], ''[From]'' AS [From], ''MessageType'' AS MessageType, ''[Message]'' AS [Message], ''DateCreated'' AS DateCreated 
	UNION ALL 
	SELECT CONVERT(VARCHAR(1024), MessageId), CONVERT(VARCHAR(1024), CustomerId), CONVERT(VARCHAR(1024), DealId), CONVERT(VARCHAR(1024), UserId), CONVERT(VARCHAR(1024), [To]), CONVERT(VARCHAR(1024), [From]), CONVERT(VARCHAR(1024), MessageType), CONVERT(VARCHAR(8000), [Message]), CONVERT(VARCHAR(1024), DateCreated) FROM ##messageDetails) AS CombinedMessageDetails'

--Create CSV
SET @bcpCommand = 'bcp "' + @messagesQuery + '" queryout "' + @messagesPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand

DROP TABLE ##messageDetails

----------------------------------------
----Opt Out		   
----------------------------------------
CREATE TABLE ##customerOptOut (
    CustomerId VARCHAR(1024),
    OptedOutEmail VARCHAR(1024),
    ooPhone VARCHAR(1024),
    ooSnailMail VARCHAR(1024),
    ooText VARCHAR(1024),
    TextingOptIn VARCHAR(1024)
)

SET @sql = N'
INSERT INTO ##customerOptOut (CustomerId, OptedOutEmail, ooPhone, ooSnailMail, ooText, TextingOptIn)
SELECT
    c.[GUID] AS CustomerId,
    CASE
        WHEN ooEmail.fkCustomerID IS NOT NULL THEN 1 ELSE 0
    END AS OptedOutEmail,
	 CASE
        WHEN ooPhone.fkCustomerID IS NOT NULL THEN 1 ELSE 0
    END AS ooPhone,
	CASE
        WHEN ooSnailMail.fkCustomerID IS NOT NULL THEN 1 ELSE 0
    END AS ooSnailMail,
	CASE
        WHEN ooText.fkCustomerID IS NOT NULL THEN 1 ELSE 0
    END AS ooText,
	CASE
        WHEN TextingOptIn.fkCustomerID IS NOT NULL THEN 1 ELSE 0
    END AS TextingOptIn
FROM
    [' + @IP + '].[' + @leadcrumb + '].dbo.Customer c (NOLOCK)
LEFT JOIN (
    SELECT fkCustomerID
    FROM [' + @IP + '].[' + @leadcrumb + '].dbo.OptOut
    WHERE OptOutType = 1
) ooEmail ON ooEmail.fkCustomerID = c.pkCustomerID

LEFT JOIN (
    SELECT fkCustomerID
    FROM [' + @IP + '].[' + @leadcrumb + '].dbo.OptOut
    WHERE OptOutType = 2
) ooPhone ON ooPhone.fkCustomerID = c.pkCustomerID
LEFT JOIN (
    SELECT fkCustomerID
    FROM [' + @IP + '].[' + @leadcrumb + '].dbo.OptOut
    WHERE OptOutType = 3
) ooSnailMail ON ooSnailMail.fkCustomerID = c.pkCustomerID
LEFT JOIN (
    SELECT fkCustomerID
    FROM [' + @IP + '].[' + @leadcrumb + '].dbo.OptOut
    WHERE OptOutType = 4
) ooText ON ooText.fkCustomerID = c.pkCustomerID
LEFT JOIN (
    SELECT fkCustomerID
    FROM [' + @IP + '].[' + @leadcrumb + '].dbo.[CustomerAttribute] ca (NOLOCK)
    WHERE	ca.[Key] = ''TextingOptIn''
) TextingOptIn ON TextingOptIn.fkCustomerID = c.pkCustomerID
WHERE
    c.fkStoreID = @StoreID
    AND c.IsDeleted = 0
    AND c.DateCreated BETWEEN @StartDate AND @EndDate
'
EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @optoutPath VARCHAR(255) = @folderPath + '\optout.csv'

-- Prepare the query for bcp
DECLARE @optoutQuery NVARCHAR(4000)
SET @optoutQuery = 'SELECT CustomerId, OptedOutEmail, ooPhone, ooSnailMail, ooText, TextingOptIn FROM (SELECT ''CustomerId'' AS CustomerId, ''OptedOutEmail'' AS OptedOutEmail, ''ooPhone'' AS ooPhone, ''ooSnailMail'' AS ooSnailMail, ''ooText'' AS ooText, ''TextingOptIn'' AS TextingOptIn UNION ALL SELECT CONVERT(VARCHAR(1024), CustomerId), CONVERT(VARCHAR(1024), OptedOutEmail), CONVERT(VARCHAR(1024), ooPhone), CONVERT(VARCHAR(1024), ooSnailMail), CONVERT(VARCHAR(1024), ooText), CONVERT(VARCHAR(1024), TextingOptIn) FROM ##customerOptOut) AS CombinedCustomerOptOut'

--Create CSV
SET @bcpCommand = 'bcp "' + @optoutQuery + '" queryout "' + @optoutPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand

DROP TABLE ##customerOptOut


----------------------------------------
----Tasks		   
----------------------------------------

CREATE TABLE ##taskDetails (
    CustomerId VARCHAR(1024),
    DealId VARCHAR(1024),
    UserId VARCHAR(1024),
    ActionTaken VARCHAR(1024),
    Resolution VARCHAR(8000),
    DateCompleted VARCHAR(100),
    ResultType VARCHAR(1024)
)


SET @sql = N'
INSERT INTO ##taskDetails (CustomerId, DealId, UserId, ActionTaken, Resolution, DateCompleted, ResultType)
SELECT	c.[GUID] AS CustomerId,
		d.[GUID] AS DealId,
		u.[GUID] AS UserId,
		CASE 
			WHEN t.ActionType = 1 THEN ''Email''
			WHEN t.ActionType = 2 THEN ''Phone''
			WHEN t.ActionType = 3 THEN ''Snail Mail''
			WHEN t.ActionType = 4 THEN ''Text''
			WHEN t.ActionType = 7 THEN ''Touch''
			ELSE ''Other''
		END AS ActionTaken,
		dbo.fnParseOnlyAlphaWithSpaces(t.Resolution) AS Resolution,
		t.DateCompleted,
		CASE WHEN t.ResultType IN (1,4) THEN ''MadeContact'' WHEN t.ResultType IN (2,3) THEN ''No Answer'' ELSE '''' END AS ResultType
FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.[Task] t (NOLOCK)
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[Customer] c (NOLOCK) ON c.pkCustomerID = t.fkCustomerID AND c.IsDeleted = 0
		INNER JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.[User] u (NOLOCK) ON u.pkUserID = t.fkCompletedByUserID
		LEFT JOIN [' + @IP + '].[' + @leadcrumb + '].dbo.Deal d (NOLOCK) ON d.pkDealID = t.fkDealID
WHERE	t.CompletionPercentage = 100
		AND t.fkStoreID = @StoreID
		AND t.DateCreated BETWEEN @StartDate AND @EndDate
'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @tasksPath VARCHAR(255) = @folderPath + '\tasks.csv'

-- Prepare the query for bcp
DECLARE @tasksQuery NVARCHAR(4000)
SET @tasksQuery = 'SELECT CustomerId, DealId, UserId, ActionTaken, Resolution, DateCompleted, ResultType FROM (SELECT ''CustomerId'' AS CustomerId, ''DealId'' AS DealId, ''UserId'' AS UserId, ''ActionTaken'' AS ActionTaken, ''Resolution'' AS Resolution, ''DateCompleted'' AS DateCompleted, ''ResultType'' AS ResultType UNION ALL SELECT CONVERT(VARCHAR(1024), CustomerId), CONVERT(VARCHAR(1024), DealId), CONVERT(VARCHAR(1024), UserId), CONVERT(VARCHAR(1024), ActionTaken), CONVERT(VARCHAR(8000), Resolution), CONVERT(VARCHAR(100), DateCompleted), CONVERT(VARCHAR(1024), ResultType) FROM ##taskDetails) AS CombinedTaskDetails'

--Create CSV
SET @bcpCommand = 'bcp "' + @tasksQuery + '" queryout "' + @tasksPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand

DROP TABLE ##taskDetails

----------------------------------------
----Users	   
----------------------------------------
CREATE TABLE ##users (
    UserId VARCHAR(1024),
    FirstName VARCHAR(1024),
    LastName VARCHAR(1024),
    Email VARCHAR(100),
    DMSIdentifier VARCHAR(1024)
)

SET @sql = N'
INSERT INTO ##users (UserId, FirstName, LastName, Email, DMSIdentifier)
SELECT	[GUID] AS UserId,
		dbo.fnParseOnlyAlphaWithSpaces(FirstName) AS FirstName,
		dbo.fnParseOnlyAlphaWithSpaces(LastName) AS LastName,
		LTRIM(RTRIM(u.Email)) AS Email,
		u.DMSIdentifier
FROM	[' + @IP + '].[' + @leadcrumb + '].dbo.[UserView] u (NOLOCK)
WHERE	u.IsDeleted = 0
		AND u.LastName NOT LIKE ''%promax%''
		AND u.UserType <> 5
		AND (u.fkStoreID = @StoreID OR u.pkUserID IN (SELECT fkUserID FROM [' + @IP + '].[' + @leadcrumb + '].dbo.StoreUser WHERE IsActive = 1 AND fkStoreID = @StoreID))
ORDER BY LastName, FirstName
'

EXEC sp_executesql @sql, N'@StoreID VARCHAR(5), @StartDate DATETIME, @EndDate DATETIME', @StoreID, @StartDate, @EndDate

--Declare File Path
DECLARE @usersPath VARCHAR(255) = @folderPath + '\users.csv'

-- Prepare the query for bcp
DECLARE @usersQuery NVARCHAR(4000)
SET @usersQuery = 'SELECT UserId, FirstName, LastName, Email, DMSIdentifier FROM (SELECT ''UserId'' AS UserId, ''FirstName'' AS FirstName, ''LastName'' AS LastName, ''Email'' AS Email, ''DMSIdentifier'' AS DMSIdentifier UNION ALL SELECT CONVERT(VARCHAR(1024), UserId), CONVERT(VARCHAR(1024), FirstName), CONVERT(VARCHAR(1024), LastName), CONVERT(VARCHAR(100), Email), CONVERT(VARCHAR(1024), DMSIdentifier) FROM ##users) AS CombinedUsers'

--Create CSV
SET @bcpCommand = 'bcp "' + @usersQuery + '" queryout "' + @usersPath + '" -c -t, -T -S ' + @@SERVERNAME
EXEC xp_cmdshell @bcpCommand

DROP TABLE ##users

END
GO


