/*
DESCRIPTION:
    Union all HPD source data (HNY and historic)

*/

-- HPD units data
ALTER TABLE hpd_hny_units_by_building ADD COLUMN ogc_fid_text text;
UPDATE hpd_hny_units_by_building
SET ogc_fid_text = 'hny' || '-' || cast(ogc_fid AS text);
ALTER TABLE hpd_hny_units_by_building DROP COLUMN ogc_fid;
ALTER TABLE hpd_hny_units_by_building RENAME COLUMN ogc_fid_text TO ogc_fid;
-- Some datasets had an erroneous dummy "wkb_geometry" column
-- This can be dropped after 25Q4. Leaving for now in case we want to rebuild 25Q2 with same source data
ALTER TABLE hpd_hny_units_by_building DROP COLUMN IF EXISTS wkb_geometry;

ALTER TABLE _init_hpd_historical_units_by_building ADD COLUMN ogc_fid_text text;
UPDATE _init_hpd_historical_units_by_building
SET ogc_fid_text = 'historical' || '-' || cast(ogc_fid AS text);
ALTER TABLE _init_hpd_historical_units_by_building DROP COLUMN ogc_fid;
ALTER TABLE _init_hpd_historical_units_by_building RENAME COLUMN ogc_fid_text TO ogc_fid;

DROP TABLE IF EXISTS hpd_units_by_building;
CREATE TABLE hpd_units_by_building AS
SELECT * FROM hpd_hny_units_by_building
UNION
SELECT * FROM _init_hpd_historical_units_by_building;

-- geocoded HPD units data
ALTER TABLE hny_geocode_results ADD COLUMN uid_text text;
UPDATE hny_geocode_results
SET uid_text = 'hny' || '-' || cast(uid AS text);
ALTER TABLE hny_geocode_results DROP COLUMN uid;
ALTER TABLE hny_geocode_results RENAME COLUMN uid_text TO uid;

ALTER TABLE hpd_historical_geocode_results ADD COLUMN uid_text text;
UPDATE hpd_historical_geocode_results
SET uid_text = 'historical' || '-' || cast(uid AS text);
ALTER TABLE hpd_historical_geocode_results DROP COLUMN uid;
ALTER TABLE hpd_historical_geocode_results RENAME COLUMN uid_text TO uid;

DROP TABLE IF EXISTS hpd_geocode_results;
CREATE TABLE hpd_geocode_results AS
SELECT * FROM hny_geocode_results
UNION
SELECT * FROM hpd_historical_geocode_results;
