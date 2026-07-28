/*
DESCRIPTION:
    Union all HPD source data (HNY and historic)

*/

-- HPD's pre-publication extract pads values, formats unit counts with thousands
-- separators, and writes '-' for null. Postgres tolerates the padding but errors on
-- the other two when downstream SQL casts these columns to numeric. No-op against the
-- Open Data version, so this is safe to leave in place once that source returns.
UPDATE hpd_hny_units_by_building
SET
    census_tract = btrim(census_tract),
    extremely_low_income_units = nullif(replace(btrim(extremely_low_income_units), ',', ''), '-'),
    very_low_income_units = nullif(replace(btrim(very_low_income_units), ',', ''), '-'),
    low_income_units = nullif(replace(btrim(low_income_units), ',', ''), '-'),
    moderate_income_units = nullif(replace(btrim(moderate_income_units), ',', ''), '-'),
    middle_income_units = nullif(replace(btrim(middle_income_units), ',', ''), '-'),
    other_income_units = nullif(replace(btrim(other_income_units), ',', ''), '-'),
    studio_units = nullif(replace(btrim(studio_units), ',', ''), '-'),
    "1_br_units" = nullif(replace(btrim("1_br_units"), ',', ''), '-'),
    "2_br_units" = nullif(replace(btrim("2_br_units"), ',', ''), '-'),
    "3_br_units" = nullif(replace(btrim("3_br_units"), ',', ''), '-'),
    "4_br_units" = nullif(replace(btrim("4_br_units"), ',', ''), '-'),
    "5_br_units" = nullif(replace(btrim("5_br_units"), ',', ''), '-'),
    "6_br+_units" = nullif(replace(btrim("6_br+_units"), ',', ''), '-'),
    unknown_br_units = nullif(replace(btrim(unknown_br_units), ',', ''), '-'),
    counted_rental_units = nullif(replace(btrim(counted_rental_units), ',', ''), '-'),
    counted_homeownership_units = nullif(replace(btrim(counted_homeownership_units), ',', ''), '-'),
    all_counted_units = nullif(replace(btrim(all_counted_units), ',', ''), '-'),
    total_units = nullif(replace(btrim(total_units), ',', ''), '-');

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
