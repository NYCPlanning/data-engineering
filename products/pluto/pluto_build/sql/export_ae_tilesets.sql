DROP TABLE IF EXISTS ae_tileset_taxlot_fill CASCADE;
CREATE TABLE ae_tileset_taxlot_fill AS
SELECT
    borough_id AS borough,
    lpad(block, 5, '0')::char(5) AS block,
    lpad(lot, 4, '0')::char(4) AS lot,
    address,
    land_use_id AS "landUseId",
    -- todo: should not just randomly take first
    ST_Transform(ST_GeometryN(wgs84, 1), 3857) AS geom
FROM ae_tax_lot
WHERE wgs84 IS NOT NULL AND NOT ST_IsEmpty(wgs84);

CREATE INDEX ae_tileset_taxlot_fill_idx ON ae_tileset_taxlot_fill USING gist (geom);

DROP VIEW IF EXISTS ae_tileset_taxlot_label;
CREATE VIEW ae_tileset_taxlot_label AS
SELECT
    t.borough,
    t.block,
    t.lot,
    t.address,
    t."landUseId",
    mic.center AS geom
FROM ae_tileset_taxlot_fill AS t
CROSS JOIN LATERAL ST_MaximumInscribedCircle(t.geom) AS mic;

DROP TABLE IF EXISTS ae_tileset_zoningdistrict_fill CASCADE;
CREATE TABLE ae_tileset_zoningdistrict_fill AS
WITH regrouped AS (
    SELECT
        zoning_district_id,
        (array_agg(zoning_district_class_id) FILTER (WHERE zoning_district_class_id LIKE 'C%'))[1] AS commercial,
        (array_agg(zoning_district_class_id) FILTER (WHERE zoning_district_class_id LIKE 'M%'))[1] AS manufacturing,
        (array_agg(zoning_district_class_id) FILTER (WHERE zoning_district_class_id LIKE 'R%'))[1] AS residential
    FROM ae_zoning_district_zoning_district_class
    GROUP BY zoning_district_id
)
SELECT
    zd.id,
    zd.label AS district,
    r.commercial,
    r.manufacturing,
    r.residential,
    ST_Transform(zd.wgs84, 3857) AS geom
FROM ae_zoning_district AS zd
INNER JOIN regrouped AS r ON zd.id = r.zoning_district_id;

CREATE INDEX ae_tileset_zoningdistrict_fill_idx ON ae_tileset_zoningdistrict_fill USING gist (geom);

DROP VIEW IF EXISTS ae_tileset_zoningdistrict_label;
CREATE VIEW ae_tileset_zoningdistrict_label AS
SELECT
    t.id,
    t.district,
    t.commercial,
    t.manufacturing,
    t.residential,
    mic.center AS geom
FROM ae_tileset_zoningdistrict_fill AS t
CROSS JOIN LATERAL ST_MaximumInscribedCircle(t.geom) AS mic;
