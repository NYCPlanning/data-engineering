-- geometries from agency project datasets that carry the project's FMS ID
WITH ddc_buildings AS (
    SELECT
        '850' || projectid AS maprojid,
        'ddc_capitalprojects_publicbuildings'::text AS candidate,
        'ddc'::text AS geomsource,
        ST_MULTI(ST_UNION(ST_CENTROID(wkb_geometry))) AS geom
    FROM {{ ref('stg__ddc_capitalprojects_publicbuildings') }}
    GROUP BY projectid
),

ddc_infrastructure AS (
    SELECT
        '850' || projectid AS maprojid,
        'ddc_capitalprojects_infrastructure'::text AS candidate,
        'ddc'::text AS geomsource,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__ddc_capitalprojects_infrastructure') }}
    GROUP BY projectid
),

edc_ferry AS (
    SELECT
        fmsid AS maprojid,
        'edc_capitalprojects_ferry'::text AS candidate,
        'edc'::text AS geomsource,
        wkb_geometry AS geom
    FROM {{ ref('stg__edc_capitalprojects_ferry') }}
),

edc_capitalprojects AS (
    SELECT
        REPLACE(pjid, ' ', '') AS maprojid,
        'edc_capitalprojects'::text AS candidate,
        'edc'::text AS geomsource,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__edc_capitalprojects') }}
    GROUP BY pjid
),

dpr_capitalprojects AS (
    SELECT
        REPLACE(fmsid, ' ', '') AS maprojid,
        'dpr_capitalprojects'::text AS candidate,
        'dpr'::text AS geomsource,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__dpr_capitalprojects') }}
    WHERE NOT COALESCE(lat = 0 OR lon = 0, FALSE)
    GROUP BY REPLACE(fmsid, ' ', '')
),

dot_streets AS (
    SELECT
        fmsagencyid || fmsid AS maprojid,
        'dot_projects_streets'::text AS candidate,
        'dot'::text AS geomsource,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__dot_projects_streets') }}
    GROUP BY fmsagencyid, fmsid
),

dot_intersections AS (
    SELECT
        fmsagencyid || fmsid AS maprojid,
        'dot_projects_intersections'::text AS candidate,
        'dot'::text AS geomsource,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__dot_projects_intersections') }}
    GROUP BY fmsagencyid, fmsid
),

dot_bridges AS (
    SELECT
        REGEXP_REPLACE(fms_id, ' ', '') AS maprojid,
        'dot_projects_bridges'::text AS candidate,
        'dot'::text AS geomsource,
        wkb_geometry::geometry AS geom
    FROM {{ source('cpdb_legacy_pipeline', 'dot_projects_bridges_byfms') }}
),

all_candidates AS (
    SELECT * FROM ddc_buildings
    UNION ALL
    SELECT * FROM ddc_infrastructure
    UNION ALL
    SELECT * FROM edc_ferry
    UNION ALL
    SELECT * FROM edc_capitalprojects
    UNION ALL
    SELECT * FROM dpr_capitalprojects
    UNION ALL
    SELECT * FROM dot_streets
    UNION ALL
    SELECT * FROM dot_intersections
    UNION ALL
    SELECT * FROM dot_bridges
)

SELECT
    maprojid,
    candidate,
    geomsource,
    geomsource AS datasource,
    candidate AS dataname,
    geom
FROM all_candidates
WHERE geom IS NOT NULL
