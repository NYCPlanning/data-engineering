-- only need hunits, geography id for joining to tables of units aggregated by geography. 
-- geotype included for sanity check, but the join column is unique across geographies
-- as written, if boroughs were included there would be collision in unique ids between council district code and borough code
DROP TABLE IF EXISTS census2020_housing_units_by_geography;
CREATE TABLE census2020_housing_units_by_geography AS
SELECT
    geotype,
    CASE
        WHEN geotype = 'CT2020' THEN ((bct2020::numeric)::int)::text
        WHEN geotype = 'CCD2023' THEN substring(geoid, 9)
        ELSE geoid
    END AS aggregate_join,
    coalesce(hunits::int, 0) AS hunits
FROM dcp_censusdata
WHERE geotype IN ('CT2020', 'CD', 'CCD2023', 'NTA2020')
UNION ALL
SELECT
    geogtype AS geotype,
    geoid20 AS aggregate_join,
    COALESCE("hunits.1"::INT, 0) AS hunits --noqa
FROM dcp_censusdata_blocks
WHERE geogtype = 'CB2020';

-- CDTAs not included in census, so join and aggregate. NTAs nest perfectly into CDTAs
-- TODO when data library is redone, great opportunity for a persisted intermediate table
INSERT INTO census2020_housing_units_by_geography
SELECT
    'CDTA2020' AS geotype,
    cdta2020 AS aggregate_join,
    sum(hunits) AS hunits
FROM
    census2020_housing_units_by_geography AS census
INNER JOIN dcp_nta2020 AS geog ON census.aggregate_join = geog.nta2020
GROUP BY cdta2020;
