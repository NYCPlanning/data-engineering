DROP TABLE IF EXISTS censusdata_cleaned;

CREATE TABLE censusdata_cleaned AS
SELECT 
    geotype,
    CASE 
        WHEN geotype = 'CT2020' THEN bct2020
        WHEN geotype = 'CnclDist2013' THEN SUBSTRING(geoid, 9)
        ELSE geoid 
    END AS aggregate_join,
    coalesce(hunits_20::int, 0) AS hunits
FROM dcp_censusdata
WHERE geotype IN ('CT2020', 'CD', 'CnclDist2013', 'NTA2020')
UNION ALL 
SELECT 
    geogtype AS geotype,
    geoid20 AS aggregate_join,
    coalesce("hunits.1"::int, 0) AS hunits
FROM dcp_censusdata_blocks
WHERE geogtype='CB2020';

INSERT INTO censusdata_cleaned 
    SELECT 
        'CDTA2020' AS geotype,
        cdta2020 AS aggregate_join,
        SUM(hunits) AS hunits
    FROM 
        censusdata_cleaned census
        INNER JOIN dcp_nta2020 geog ON census.aggregate_join = geog.nta2020
    GROUP BY cdta2020;