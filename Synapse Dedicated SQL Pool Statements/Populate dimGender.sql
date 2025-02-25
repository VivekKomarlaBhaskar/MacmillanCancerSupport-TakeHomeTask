/* Insert sex only if it does not already exist. */
DECLARE @MAX_genderKey INT;
SELECT @MAX_genderKey = ISNULL(MAX(genderKey),0) FROM [MacmillanCancerSupport_Test].[dimGender];

WITH Distinct_sex AS (
    SELECT DISTINCT
        sex
    FROM [MacmillanCancerSupport_Test].[vwCancerRegistrations]
)
INSERT INTO [MacmillanCancerSupport_Test].[dimGender]
SELECT @MAX_genderKey + ROW_NUMBER() OVER(ORDER BY Diss.sex ASC), Diss.sex FROM Distinct_sex Diss
WHERE NOT EXISTS (
    SELECT 1
    FROM [MacmillanCancerSupport_Test].[dimGender] dG
    WHERE dG.sex = Diss.sex
);