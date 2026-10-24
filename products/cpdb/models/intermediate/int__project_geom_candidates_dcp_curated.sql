-- geometries DCP assigned to projects by hand or with one-off scripts, kept as seeds and a
-- static SQL dump. Their legacy `datasource` and `dataname` are null.
WITH manual_geoms_2020 AS (
    SELECT
        maprojid,
        'dcp_manual_geoms_2020'::text AS candidate,
        CASE
            WHEN footprint_project_geomsource ~* 'AD Sprint' THEN 'EP 2020'
            ELSE footprint_project_geomsource
        END AS geomsource,
        ST_SETSRID(ST_GEOMFROMTEXT(geometry), 4326) AS geom
    FROM {{ ref('dcp_manual_geoms_2020') }}
    WHERE geometry IS NOT NULL
),

id_bin_map AS (
    SELECT
        m.maprojid,
        'dcp_id_bin_map'::text AS candidate,
        'footprint_script'::text AS geomsource,
        ST_CENTROID(b.wkb_geometry) AS geom
    FROM {{ ref('dcp_id_bin_map') }} AS m
    INNER JOIN {{ ref('stg__doitt_buildingfootprints') }} AS b ON m.bin::bigint = b.bin::bigint
    WHERE b.wkb_geometry IS NOT NULL
),

dcp_json AS (
    SELECT
        maprojid,
        'dcp_json'::text AS candidate,
        'DCP_geojson'::text AS geomsource,
        ST_SETSRID(geom::geometry, 4326) AS geom
    FROM {{ ref('dcp_json') }}
    WHERE geom IS NOT NULL
),

sprints AS (
    SELECT
        maprojid,
        'sprints'::text AS candidate,
        geomsource,
        geom
    FROM {{ source('cpdb_legacy_pipeline', 'sprints') }}
    WHERE geom IS NOT NULL
),

all_candidates AS (
    SELECT * FROM manual_geoms_2020
    UNION ALL
    SELECT * FROM id_bin_map
    UNION ALL
    SELECT * FROM dcp_json
    UNION ALL
    SELECT * FROM sprints
)

SELECT
    maprojid,
    candidate,
    geomsource,
    NULL::text AS datasource,
    NULL::text AS dataname,
    geom
FROM all_candidates
