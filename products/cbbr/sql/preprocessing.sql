-- create versions of relvant import data to modify
DROP TABLE IF EXISTS _cbbr_submissions;

CREATE TABLE _cbbr_submissions AS
SELECT
    unique_id,
    trkno AS tracking_code,
    borough,
    RIGHT(cb_label, 2) AS cd,
    cb_label,
    "type" AS type_br,
    priority,
    need,
    request,
    REPLACE(reason, E'\n', ' ') AS explanation,
    location,
    loc_type AS "type",
    supported_by_1 AS supporters_1,
    supported_by_2 AS supporters_2,
    project_id_1,
    project_id_2,
    project_id_3,
    budget_line_1,
    budget_line_2,
    budget_line_3,
    agency_acronym,
    agency,
    agy_response_category AS agency_category_response,
    agy_response AS agency_response,
    REPLACE(additional_comment, E'\n', ' ') AS additional_comment,
    NULL AS parent_tracking_code
FROM cbbr_submissions;

DROP TABLE IF EXISTS _doitt_buildingfootprints;
CREATE TABLE _doitt_buildingfootprints AS TABLE doitt_buildingfootprints;
ALTER TABLE _doitt_buildingfootprints RENAME COLUMN wkb_geometry TO geom;

DROP TABLE IF EXISTS _dpr_parksproperties;
CREATE TABLE _dpr_parksproperties AS TABLE dpr_parksproperties;
ALTER TABLE _dpr_parksproperties RENAME COLUMN wkb_geometry TO geom;

DROP TABLE IF EXISTS _dcp_facilities;
CREATE TABLE _dcp_facilities AS TABLE dcp_facilities;
ALTER TABLE _dcp_facilities RENAME COLUMN wkb_geometry TO geom;

DROP TABLE IF EXISTS _cbbr_point_corrections;
CREATE TABLE _cbbr_point_corrections AS TABLE cbbr_point_corrections;
ALTER TABLE _cbbr_point_corrections RENAME COLUMN wkt TO geom;
UPDATE _cbbr_point_corrections SET geom = ST_GEOMETRYFROMTEXT(geom, 4326);

DROP TABLE IF EXISTS _cbbr_line_corrections;
CREATE TABLE _cbbr_line_corrections AS TABLE cbbr_line_corrections;
ALTER TABLE _cbbr_line_corrections RENAME COLUMN wkt TO geom;
UPDATE _cbbr_line_corrections SET geom = ST_GEOMETRYFROMTEXT(geom, 4326);

DROP TABLE IF EXISTS _cbbr_poly_corrections;
CREATE TABLE _cbbr_poly_corrections AS TABLE cbbr_poly_corrections;
ALTER TABLE _cbbr_poly_corrections RENAME COLUMN wkt TO geom;
UPDATE _cbbr_poly_corrections SET geom = ST_GEOMETRYFROMTEXT(geom, 4326);

-- -- extract geometries from manually researched json files
-- UPDATE
--     cbbr_geoms
-- SET
--     geom = ST_SetSRID (ST_GeomFromGeoJSON (geom), 4326);
-- ALTER TABLE cbbr_geoms
--     ALTER COLUMN geom TYPE GEOMETRY;
--
-- create unique_id column
