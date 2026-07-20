WITH base AS (
    SELECT * FROM {{ ref('int__dcpattributes_geom_facilities') }}
),

geomclean_step1 AS (
    SELECT
        maprojid,
        CASE
            WHEN ST_GEOMETRYTYPE(geom) = 'ST_MultiLineString' THEN ST_SNAPTOGRID(geom, .00001)
            ELSE geom
        END AS geom
    FROM base
),

geomclean_step2 AS (
    SELECT
        maprojid,
        CASE
            WHEN ST_GEOMETRYTYPE(geom) = 'ST_MultiLineString' THEN ST_BUFFER(geom::geography, 15)::geometry
            ELSE geom
        END AS geom
    FROM geomclean_step1
),

geomclean AS (
    SELECT
        maprojid,
        CASE
            WHEN ST_GEOMETRYTYPE(geom) IN ('ST_Polygon', 'ST_Point') THEN ST_MULTI(geom)
            ELSE geom
        END AS geom
    FROM geomclean_step2
),

badgeoms_flagged AS (
    SELECT DISTINCT maprojid FROM {{ ref('cpdb_badgeoms') }}
),

agencyverified_never_mappable AS (
    SELECT DISTINCT maprojid FROM {{ ref('dcp_cpdb_agencyverified') }}
    WHERE mappable = 'No - Can never be mapped'
),

agencyverified_maybe_mappable AS (
    SELECT DISTINCT maprojid FROM {{ ref('dcp_cpdb_agencyverified') }}
    WHERE mappable = 'No - Can be in future'
),

-- legacy's own trusted-source exception here -- `geomsource != 'dpr' OR geomsource != 'dot' OR
-- geomsource != 'ddc'` -- is a tautology (always true for any single value), so the "don't null
-- out trusted-agency geoms" protection never actually fires. Fixed here per an earlier decision
-- to not replicate the bug: `NOT IN` instead of chained `OR !=`.
badgeoms AS (
    SELECT
        base.maprojid,
        CASE
            WHEN
                base.maprojid IN (SELECT maprojid FROM badgeoms_flagged)
                AND base.geomsource NOT IN ('dpr', 'dot', 'ddc')
                THEN NULL
            WHEN base.maprojid IN (SELECT maprojid FROM agencyverified_never_mappable)
                THEN NULL
            WHEN
                base.maprojid IN (SELECT maprojid FROM agencyverified_maybe_mappable)
                AND base.geomsource NOT IN ('dpr', 'dot', 'ddc', 'edc')
                THEN NULL
            ELSE geomclean.geom
        END AS geom
    FROM base
    INNER JOIN geomclean ON base.maprojid = geomclean.maprojid
),

-- fix inverted longitude for point projects with at least one point outside NYC
lon_fix_candidates AS (
    SELECT
        maprojid,
        geom
    FROM badgeoms
    WHERE ST_GEOMETRYTYPE(geom) = 'ST_MultiPoint' AND ST_XMAX(geom) > 0
),

lon_fixed_points AS (
    SELECT
        candidates.maprojid,
        ST_MULTI(ST_UNION(
            CASE
                WHEN ST_X(pt.geom) > 0
                    THEN ST_SETSRID(ST_MAKEPOINT(-ST_X(pt.geom), ST_Y(pt.geom)), 4326)
                ELSE pt.geom
            END
        )) AS geom
    FROM lon_fix_candidates AS candidates
    CROSS JOIN LATERAL ST_DUMP(candidates.geom) AS pt
    GROUP BY candidates.maprojid
)

SELECT
    base.ccpversion,
    base.maprojid,
    base.magency,
    base.magencyacro,
    base.projectid,
    base.description,
    base.typecategory,
    base.geomsource,
    base.dataname,
    base.datasource,
    base.datadate,
    COALESCE(lon_fixed_points.geom, badgeoms.geom) AS geom
FROM base
INNER JOIN badgeoms ON base.maprojid = badgeoms.maprojid
LEFT JOIN lon_fixed_points ON base.maprojid = lon_fixed_points.maprojid
