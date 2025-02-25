/* Insert statement for factCancerRegistrations table*/
INSERT INTO [MacmillanCancerSupport_Test].[factCancerRegistrations]
SELECT dimICD.icdKey,
       dimGender.genderKey,
       dimRegion.regionKey,
       vwCancerRegistrations.registrationTotals,
       vwCancerRegistrations.ReportDate
FROM [MacmillanCancerSupport_Test].[vwCancerRegistrations] vwCancerRegistrations
JOIN [MacmillanCancerSupport_Test].[dimICD] dimICD ON vwCancerRegistrations.icdCode = dimICD.icdCode AND dimICD.endDate IS NULL
JOIN [MacmillanCancerSupport_Test].[dimGender] dimGender ON vwCancerRegistrations.sex = dimGender.sex
JOIN [MacmillanCancerSupport_Test].[dimRegion] dimRegion ON vwCancerRegistrations.regionCode = dimRegion.regionCode AND dimRegion.endDate IS NULL;