-- remove faulty geometries from attributes table

-- create table of faulty geometries
DROP TABLE IF EXISTS cbbr_cdwide;
CREATE TABLE cbbr_cdwide (
regid text
);

COPY cbbr_cdwide FROM '/prod/db-cbbr/cbbr_build/cbbr_cdwide.csv' DELIMITER ',' CSV;


-- Remove
UPDATE cbbr_submissions
SET site1 = 'No, this is a request for our community more generally'
WHERE regid in (SELECT DISTINCT regid FROM cbbr_cdwide)
;