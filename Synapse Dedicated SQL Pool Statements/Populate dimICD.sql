--Populating Tables--dimICD--
/* Assuming we receive files daily and process each day's file individually and sequentially. */
/* endDate existing records to allow changes while retaining the existing ICD metadata, using icdCode as the business key. */

/* Update endDate to reportDate for the existing icdCode record if there are any changes to icdDescription. */
WITH Distinct_icd AS (
    SELECT DISTINCT
        icdCode, 
        icdDescription, 
        reportDate
    FROM [MacmillanCancerSupport_Test].[vwCancerRegistrations]
)
UPDATE dimicd
SET dimicd.endDate = Disicd.reportDate                                                                                         
FROM [MacmillanCancerSupport_Test].[dimICD] dimicd
JOIN Distinct_icd Disicd
ON dimicd.icdCode = Disicd.icdCode
AND endDate IS NULL
WHERE dimicd.icdDescription != Disicd.icdDescription;


/*  Insert new icd codes, or for existing icd codes, insert a new record if icdDescription has changed. */
DECLARE @MAX_icdKey INT;
SELECT @MAX_icdKey = ISNULL(MAX(icdKey),0) FROM [MacmillanCancerSupport_Test].[dimICD];

WITH Distinct_icd AS (
    SELECT DISTINCT
        icdCode,      
        icdDescription, 
        reportDate
    FROM [MacmillanCancerSupport_Test].[vwCancerRegistrations]
)
INSERT INTO [MacmillanCancerSupport_Test].[dimICD]
SELECT @MAX_icdKey + ROW_NUMBER() OVER(ORDER BY Disicd.icdCode ASC), 
        Disicd.icdCode, Disicd.icdDescription, Disicd.reportDate AS startDate, NULL AS endDate
FROM Distinct_icd Disicd
LEFT JOIN [MacmillanCancerSupport_Test].[dimICD] dimicd
ON dimicd.icdCode = Disicd.icdCode AND dimicd.endDate IS NULL
WHERE (dimicd.icdCode IS NULL OR dimicd.icdDescription != Disicd.icdDescription);