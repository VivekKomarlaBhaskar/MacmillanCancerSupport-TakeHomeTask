CREATE SCHEMA [MacmillanCancerSupport_Test];

---------------------------------------Staging Table---------------------------------------------

--1. tblStagingCancerRegistrations External Table

--DROP EXTERNAL TABLE [MacmillanCancerSupport_Test].[tblStagingCancerRegistrations];
--DELETE FROM [MacmillanCancerSupport_Test].[tblStagingCancerRegistrations];
CREATE EXTERNAL TABLE [MacmillanCancerSupport_Test].[tblStagingCancerRegistrations]
(
    icdCode VARCHAR(50) NOT NULL,
    icdDescription VARCHAR(100) NOT NULL,
    sex VARCHAR(10) NOT NULL,
    regionName VARCHAR(100) NOT NULL,
    registrationTotals BIGINT NOT NULL,
    reportDate DATE NOT NULL
)
WITH (
 LOCATION = '/Test/Inbound/MacmillanCancerSupport_Test/CancerRegistrationBySexAndRegion/SynapseProcessing',
    DATA_SOURCE = GBNewsAnalyticsGen2TestDataSource,
    FILE_FORMAT = DelimitedText_CommaTerminator_QuoteDelimiter_FirstRowAsHeader
);

SELECT * FROM [MacmillanCancerSupport_Test].[tblStagingCancerRegistrations];



---------------------------------------View on staging table---------------------------------------------

--1. vwCancerRegistrations View

--DROP VIEW [MacmillanCancerSupport_Test].[vwCancerRegistrations];
--CREATE VIEW [MacmillanCancerSupport_Test].[vwCancerRegistrations] AS 
SELECT
   icdCode,
   icdDescription,
   REPLACE(REPLACE(LOWER(sex), 'females', 'Female'), 'males', 'Male') AS sex,
   TRIM(RIGHT(regionName, 9)) AS regionCode,
   TRIM(LEFT(regionName, LEN(regionName) - 9)) AS regionName,
   registrationTotals,
   ReportDate 
FROM
   [MacmillanCancerSupport_Test].[tblStagingCancerRegistrations] 
WHERE
   icdCode NOT LIKE '%-%' 
   AND lower(regionName) <> 'england'
;

SELECT * FROM [MacmillanCancerSupport_Test].[vwCancerRegistrations];



---------------------------------------Dimension Tables--------------------------------------------------

--1. dimRegion Table

--DROP TABLE [MacmillanCancerSupport_Test].[dimRegion];
--DELETE FROM [MacmillanCancerSupport_Test].[dimRegion];
CREATE TABLE [MacmillanCancerSupport_Test].[dimRegion]
(
    regionKey INT NOT NULL,
    regionCode VARCHAR(9) NOT NULL,
    regionName VARCHAR(100) NOT NULL,
    startDate DATE NOT NULL,
    endDate DATE NULL
)
WITH
(
    DISTRIBUTION = REPLICATE
);

SELECT * FROM [MacmillanCancerSupport_Test].[dimRegion];



--2. dimICD Table

--DROP TABLE [MacmillanCancerSupport_Test].[dimICD];
--DELETE FROM [MacmillanCancerSupport_Test].[dimICD];
/* Assumtion: Considered to be Type-2 SCD on an assumption that the descriptions/code can change in each revision, currently the revision is 10*/
CREATE TABLE [MacmillanCancerSupport_Test].[dimICD]
(
    icdKey INT NOT NULL,
    icdCode VARCHAR(10) NOT NULL,
    icdDescription VARCHAR(100) NOT NULL,
    startDate DATE NOT NULL,
    endDate DATE NULL
)
WITH
(
    DISTRIBUTION = REPLICATE
);

SELECT * FROM [MacmillanCancerSupport_Test].[dimICD];



--3. dimGender Table

--DROP TABLE [MacmillanCancerSupport_Test].[dimGender];
--DELETE FROM [MacmillanCancerSupport_Test].[dimGender];
/* Assumtion: Considered to be Type-1 SCD*/
CREATE TABLE [MacmillanCancerSupport_Test].[dimGender]
(
    genderKey INT NOT NULL,
    sex VARCHAR(10) NOT NULL
)
WITH
(
    DISTRIBUTION = REPLICATE
);

SELECT * FROM [MacmillanCancerSupport_Test].[dimGender];


---------------------------------------Fact Table---------------------------------------------

--DROP TABLE [MacmillanCancerSupport_Test].[factCancerRegistrations];
--DELETE FROM [MacmillanCancerSupport_Test].[factCancerRegistrations];
CREATE TABLE [MacmillanCancerSupport_Test].[factCancerRegistrations] (
    icdKey INT NOT NULL, -- Foreign key reference to dimICD
    genderKey INT NOT NULL, -- Foreign key reference to dimGender    
    regionKey INT NOT NULL, -- Foreign key reference to dimRegion 
    cumulative_registrationTotals BIGINT NOT NULL,
    reportDate DATE NOT NULL
)
WITH (
    DISTRIBUTION = HASH (icdKey), -- Hash distributed on icdKey for performance
    HEAP
);

SELECT * FROM [MacmillanCancerSupport_Test].[factCancerRegistrations];
