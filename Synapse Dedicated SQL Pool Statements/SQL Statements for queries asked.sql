-------------------------------------::::::Queries Asked:::::-------------------------------------

-- 1. Which region has the highest overall incidence of cancer for males and females combined?

/* Assumtion: Considering only records where reportDate is the latest (MAX(reportDate))*/
WITH LatestReport AS (
    SELECT MAX(reportDate) AS maxReportDate 
    FROM MacmillanCancerSupport_Test.factCancerRegistrations
)
SELECT 
    r.regionName, 
    SUM(f.cumulative_registrationTotals) AS totalIncidence
FROM MacmillanCancerSupport_Test.factCancerRegistrations f
JOIN MacmillanCancerSupport_Test.dimRegion r 
    ON f.regionKey = r.regionKey
    AND f.reportDate >= r.startDate 
    AND r.endDate IS NULL
JOIN LatestReport lr 
    ON f.reportDate = lr.maxReportDate
GROUP BY r.regionName
ORDER BY totalIncidence DESC;


/*
Explanation:
We aggregate cumulative_registrationTotals by regionName to get the total cancer incidence per region.
The result is sorted in descending order to find the region with the highest incidence.
*/


-- 2. What are the top 3 cancers with the highest incidence rates in London for females?

/* Assumtion: Considering only records where reportDate is the latest (MAX(reportDate))*/
WITH LatestReport AS (
    SELECT MAX(reportDate) AS maxReportDate 
    FROM MacmillanCancerSupport_Test.factCancerRegistrations
)
SELECT Top 3
    i.icdDescription, 
    SUM(f.cumulative_registrationTotals) AS totalIncidence
FROM MacmillanCancerSupport_Test.factCancerRegistrations f
JOIN MacmillanCancerSupport_Test.dimICD i 
    ON f.icdKey = i.icdKey
    AND f.reportDate >= i.startDate 
    AND i.endDate IS NULL
JOIN MacmillanCancerSupport_Test.dimRegion r 
    ON f.regionKey = r.regionKey
    AND f.reportDate >= r.startDate 
    AND r.endDate IS NULL
JOIN MacmillanCancerSupport_Test.dimGender g 
    ON f.genderKey = g.genderKey
JOIN LatestReport lr 
    ON f.reportDate = lr.maxReportDate
WHERE 
    r.regionName = 'London'
    AND g.sex = 'Female'
GROUP BY i.icdDescription
ORDER BY totalIncidence DESC;
/*
Explanation:

We filter the data for the London region and Female gender.
We aggregate the cumulative_registrationTotals by icdDescription to get the total cases per cancer type.
The result is sorted in descending order to find the top 3 cancers with the highest incidence.
*/

--3. Is there a noticeable difference in the incidence of any specific cancer type between the North East and South West regions?

/* Assumtion: Considering only records where reportDate is the latest (MAX(reportDate))*/
WITH LatestReport AS (
    SELECT MAX(reportDate) AS maxReportDate 
    FROM MacmillanCancerSupport_Test.factCancerRegistrations
)
SELECT 
    i.icdDescription,
    SUM(CASE WHEN r.regionName = 'North East' THEN f.cumulative_registrationTotals ELSE 0 END) AS northEastIncidence,
    SUM(CASE WHEN r.regionName = 'South West' THEN f.cumulative_registrationTotals ELSE 0 END) AS southWestIncidence,
    ABS(
        SUM(CASE WHEN r.regionName = 'North East' THEN f.cumulative_registrationTotals ELSE 0 END) - 
        SUM(CASE WHEN r.regionName = 'South West' THEN f.cumulative_registrationTotals ELSE 0 END)
    ) AS difference
FROM MacmillanCancerSupport_Test.factCancerRegistrations f
JOIN MacmillanCancerSupport_Test.dimICD i 
    ON f.icdKey = i.icdKey
    AND f.reportDate >= i.startDate 
    AND i.endDate IS NULL
JOIN MacmillanCancerSupport_Test.dimRegion r 
    ON f.regionKey = r.regionKey
    AND f.reportDate >= r.startDate 
    AND r.endDate IS NULL
JOIN LatestReport lr 
    ON f.reportDate = lr.maxReportDate
WHERE r.regionName IN ('North East', 'South West')
GROUP BY i.icdDescription
ORDER BY difference DESC;

/*
Explanation:

We calculate total incidence per icdDescription separately for North East and South West.
We compute the absolute difference to identify significant variations.
The result is sorted in descending order to highlight cancers with the biggest differences in incidence between the two regions.
*/