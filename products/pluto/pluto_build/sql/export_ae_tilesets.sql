-- TODO - handle duplicates
DROP TABLE IF EXISTS ae_tileset_taxlot_fill CASCADE;
CREATE TABLE ae_tileset_taxlot_fill AS
SELECT
    borough_id AS borough,
    LPAD(block, 5, '0')::char(5) AS block,
    LPAD(lot, 4, '0')::char(4) AS lot,
    address,
    land_use_id AS "landUseId",
    -- todo: should not just randomly take first
    ST_TRANSFORM(ST_GEOMETRYN(wgs84, 1), 3857) AS geom
FROM ae_tax_lot
WHERE wgs84 IS NOT NULL AND NOT ST_ISEMPTY(wgs84);

CREATE INDEX ae_tileset_taxlot_fill_idx ON ae_tileset_taxlot_fill USING gist (geom);

DROP VIEW IF EXISTS ae_tileset_taxlot_label;
CREATE VIEW ae_tileset_taxlot_label AS
SELECT
    t.borough,
    t.block,
    t.lot,
    mic.center AS geom
FROM ae_tileset_taxlot_fill AS t
CROSS JOIN LATERAL ST_MAXIMUMINSCRIBEDCIRCLE(t.geom) AS mic;

DROP TABLE IF EXISTS ae_tileset_zoningdistrict_fill CASCADE;
CREATE TABLE ae_tileset_zoningdistrict_fill AS
WITH regrouped AS (
    SELECT
        zoning_district_id,
        (ARRAY_AGG(zoning_district_class_id) FILTER (WHERE zoning_district_class_id LIKE 'C%'))[1] AS commercial,
        (ARRAY_AGG(zoning_district_class_id) FILTER (WHERE zoning_district_class_id LIKE 'M%'))[1] AS manufacturing,
        (ARRAY_AGG(zoning_district_class_id) FILTER (WHERE zoning_district_class_id LIKE 'R%'))[1] AS residential
    FROM ae_zoning_district_zoning_district_class
    GROUP BY zoning_district_id
)
SELECT
    zd.id AS id,
    zd.label AS district,
    r.commercial,
    r.manufacturing,
    r.residential,
    ST_TRANSFORM(zd.wgs84, 3857) AS geom
FROM ae_zoning_district AS zd
INNER JOIN regrouped AS r ON zd.id = r.zoning_district_id;

CREATE INDEX ae_tileset_zoningdistrict_fill_idx ON ae_tileset_zoningdistrict_fill USING gist (geom);

DROP VIEW IF EXISTS ae_tileset_zoningdistrict_label;
CREATE VIEW ae_tileset_zoningdistrict_label AS
SELECT
    t.id,
    t.district,
    mic.center AS geom
FROM ae_tileset_zoningdistrict_fill AS t
CROSS JOIN LATERAL ST_MAXIMUMINSCRIBEDCIRCLE(t.geom) AS mic;
