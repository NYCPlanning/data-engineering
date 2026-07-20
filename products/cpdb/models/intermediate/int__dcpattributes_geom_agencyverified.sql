WITH base AS (
    SELECT * FROM {{ ref('int__dcpattributes_geom_unconditional') }}
),

agencyverified_base AS (
    SELECT
        ROW_NUMBER() OVER () AS row_id,
        *
    FROM {{ ref('dcp_cpdb_agencyverified') }}
),

-- gap-fill 1: DOT projects by bridge bin
dot_bin AS (
    SELECT
        a.row_id,
        b.wkb_geometry::geometry AS geom
    FROM agencyverified_base AS a
    INNER JOIN {{ source('cpdb_legacy_pipeline', 'dot_projects_bridges_byfms') }} AS b
        ON b.bin::text = a.bin::text
    WHERE a.agency = 'DOT'
),

-- gap-fill 2: DPR projects by park bin (gispropnum)
dpr_bin AS (
    SELECT
        a.row_id,
        b.wkb_geometry AS geom
    FROM agencyverified_base AS a
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON b.gispropnum::text = a.bin::text
    WHERE a.agency = 'DPR'
),

-- gap-fill 3: any project by building bin
building_bin AS (
    SELECT
        a.row_id,
        ST_CENTROID(b.wkb_geometry) AS geom
    FROM agencyverified_base AS a
    INNER JOIN {{ ref('stg__doitt_buildingfootprints') }} AS b
        ON a.bin::float::bigint = b.bin::bigint
    WHERE
        a.bin IS NOT NULL
        AND a.bin::text ~ '^[0-9]+(\.[0-9]+)?$'
        AND b.wkb_geometry IS NOT NULL
),

-- gap-fill 4: any project by bbl
mappluto_bbl AS (
    SELECT
        a.row_id,
        ST_CENTROID(b.wkb_geometry) AS geom
    FROM agencyverified_base AS a
    INNER JOIN {{ ref('stg__dcp_mappluto_wi') }} AS b
        ON a.bbl::text = b.bbl::text
    WHERE a.bbl IS NOT NULL
),

gapfill AS (
    SELECT
        a.row_id,
        COALESCE(dot_bin.geom, dpr_bin.geom, building_bin.geom, mappluto_bbl.geom) AS geom
    FROM agencyverified_base AS a
    LEFT JOIN dot_bin ON a.row_id = dot_bin.row_id
    LEFT JOIN dpr_bin ON a.row_id = dpr_bin.row_id
    LEFT JOIN building_bin ON a.row_id = building_bin.row_id
    LEFT JOIN mappluto_bbl ON a.row_id = mappluto_bbl.row_id
),

-- Python geocode output (dcp_cpdb_agencyverified_geo) legitimately has multiple rows per
-- maprojid (a project can span several real locations). Resolve each geocode row to one point
-- (lat/lon takes priority over building bin, matching legacy's run order), then union all
-- resolved points per maprojid -- this sidesteps the row-fanout ambiguity in the legacy
-- UPDATE...FROM (undefined behavior when one target row matches multiple source rows) since the
-- outer aggregation below unions across all dcp_cpdb_agencyverified rows per maprojid anyway;
-- applying the same pre-combined geometry to every row sharing a maprojid is equivalent at that
-- point, regardless of the row-to-row correspondence on either side.
geocode_bin AS (
    SELECT
        c.index,
        ST_CENTROID(b.wkb_geometry) AS geom
    FROM {{ source('cpdb_legacy_pipeline', 'dcp_cpdb_agencyverified_geo') }} AS c
    INNER JOIN {{ ref('stg__doitt_buildingfootprints') }} AS b
        ON b.bin::bigint::text = c.bin::bigint::text
    WHERE c.bin IS NOT NULL AND b.wkb_geometry IS NOT NULL
),

geocode_resolved AS (
    SELECT
        c.maprojid,
        COALESCE(
            CASE
                WHEN c.lat IS NOT NULL AND c.lon IS NOT NULL
                    THEN ST_SETSRID(ST_MAKEPOINT(c.lon::double precision, c.lat::double precision), 4326)
            END,
            geocode_bin.geom
        ) AS geom
    FROM {{ source('cpdb_legacy_pipeline', 'dcp_cpdb_agencyverified_geo') }} AS c
    LEFT JOIN geocode_bin ON c.index = geocode_bin.index
),

geocode_by_maprojid AS (
    SELECT
        maprojid,
        ST_MULTI(ST_UNION(geom)) AS geom
    FROM geocode_resolved
    WHERE geom IS NOT NULL
    GROUP BY maprojid
),

agencyverified_with_geom AS (
    SELECT
        a.maprojid,
        COALESCE(geocode_by_maprojid.geom, gapfill.geom) AS geom
    FROM agencyverified_base AS a
    LEFT JOIN gapfill ON a.row_id = gapfill.row_id
    LEFT JOIN geocode_by_maprojid ON a.maprojid = geocode_by_maprojid.maprojid
),

agencyverified AS (
    SELECT
        maprojid,
        ST_MULTI(ST_UNION(geom)) AS geom,
        'agency'::text AS geomsource,
        'agency'::text AS datasource,
        'dcp_cpdb_agencyverified'::text AS dataname
    FROM agencyverified_with_geom
    GROUP BY maprojid
    HAVING ST_MULTI(ST_UNION(geom)) IS NOT NULL
)

SELECT
    base.ccpversion,
    base.maprojid,
    base.magency,
    base.magencyacro,
    base.projectid,
    base.description,
    base.typecategory,
    COALESCE(agencyverified.geomsource, base.geomsource) AS geomsource,
    COALESCE(agencyverified.dataname, base.dataname) AS dataname,
    COALESCE(agencyverified.datasource, base.datasource) AS datasource,
    base.datadate,
    COALESCE(agencyverified.geom, base.geom) AS geom
FROM base
LEFT JOIN agencyverified ON base.maprojid = agencyverified.maprojid
