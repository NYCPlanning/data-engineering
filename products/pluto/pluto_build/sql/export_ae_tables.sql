CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS ae_zoning_district;
CREATE TABLE ae_zoning_district AS
SELECT
    GEN_RANDOM_UUID() AS id,
    zonedist AS label,
    geom AS wgs84,
    ST_TRANSFORM(geom, 2263) AS li_ft
FROM dcp_zoningdistricts
WHERE
    zonedist NOT IN ('PARK', 'BALL FIELD', 'PUBLIC PLACE', 'PLAYGROUND', 'BPC', '')
    AND ST_GEOMETRYTYPE(ST_MAKEVALID(geom)) = 'ST_MultiPolygon';
