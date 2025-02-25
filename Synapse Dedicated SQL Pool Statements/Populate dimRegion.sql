--Populating Tables--dimRegion--
/* Assuming we receive files daily and process each day's file individually and sequentially. */
/* endDate existing records to allow changes while retaining the existing region metadata, using regionCode as the business key. */

/* Update endDate to reportDate for the existing regionCode record if there are any changes to regionName. */
WITH Distinct_regions AS (
    SELECT DISTINCT
        regionCode, 
        regionName, 
        reportDate
    FROM [MacmillanCancerSupport_Test].[vwCancerRegistrations]
)
UPDATE dimR
SET dimR.endDate = Disr.reportDate                                                                                         
FROM [MacmillanCancerSupport_Test].[dimRegion] dimR
JOIN Distinct_regions Disr
ON dimR.regionCode = Disr.regionCode
AND endDate IS NULL
WHERE dimR.regionName != Disr.regionName;


/*  Insert new regions, or for existing regions, insert a new record if regionName has changed. */
DECLARE @MAX_regionKey INT;
SELECT @MAX_regionKey = ISNULL(MAX(regionKey),0) FROM [MacmillanCancerSupport_Test].[dimRegion];

WITH Distinct_regions AS (
    SELECT DISTINCT
        regionCode, 
        regionName, 
        reportDate
    FROM [MacmillanCancerSupport_Test].[vwCancerRegistrations]
)
INSERT INTO [MacmillanCancerSupport_Test].[dimRegion]
SELECT @MAX_regionKey + ROW_NUMBER() OVER(ORDER BY Disr.regionCode ASC), 
        Disr.regionCode, Disr.regionName, Disr.reportDate AS startDate, NULL AS endDate
FROM Distinct_regions Disr
LEFT JOIN [MacmillanCancerSupport_Test].[dimRegion] dimR
ON dimR.regionCode = Disr.regionCode AND dimR.endDate IS NULL
WHERE (dimR.regionCode IS NULL OR dimR.regionName != Disr.regionName);