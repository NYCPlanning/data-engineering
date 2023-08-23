SELECT agencyacro, COUNT(*)
FROM cbbr_submissions
GROUP BY agencyacro
ORDER BY count DESC;

SELECT commdist, COUNT(*)
FROM cbbr_submissions
GROUP BY commdist
ORDER BY count DESC;

SELECT name, COUNT(*)
FROM cbbr_submissions
GROUP BY name
ORDER BY count DESC;

SELECT borough, COUNT(*)
FROM cbbr_submissions
GROUP BY borough
ORDER BY count DESC;