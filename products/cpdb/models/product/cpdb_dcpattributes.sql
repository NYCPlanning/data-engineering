WITH geoms AS (
    SELECT * FROM {{ ref('int__project_geom') }}
),

geomclean_step1 AS (
    SELECT
        maprojid,
        CASE
            WHEN ST_GEOMETRYTYPE(geom) = 'ST_MultiLineString' THEN ST_SNAPTOGRID(geom, .00001)
            ELSE geom
        END AS geom
    FROM geoms
    WHERE geom_removed_reason IS NULL
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

-- fix inverted longitude for point projects with at least one point outside NYC
lon_fix_candidates AS (
    SELECT
        maprojid,
        geom
    FROM geomclean
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
    geoms.geomsource,
    geoms.dataname,
    geoms.datasource,
    NULL::timestamp AS datadate,
    COALESCE(lon_fixed_points.geom, geomclean.geom) AS geom
FROM {{ ref('int__dcpattributes_typecategory') }} AS base
INNER JOIN geoms ON base.maprojid = geoms.maprojid
LEFT JOIN geomclean ON base.maprojid = geomclean.maprojid
LEFT JOIN lon_fixed_points ON base.maprojid = lon_fixed_points.maprojid
