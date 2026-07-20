WITH base AS (
    SELECT * FROM {{ ref('int__dcpattributes_typecategory') }}
),

sprints AS (
    SELECT
        maprojid,
        geom,
        geomsource
    FROM {{ source('cpdb_legacy_pipeline', 'sprints') }}
    WHERE geom IS NOT NULL
),

dcp_json AS (
    SELECT
        maprojid,
        ST_SETSRID(geom::geometry, 4326) AS geom,
        'DCP_geojson'::text AS geomsource
    FROM {{ ref('dcp_json') }}
    WHERE geom IS NOT NULL
),

id_bin_map AS (
    SELECT
        m.maprojid,
        ST_CENTROID(b.wkb_geometry) AS geom,
        'footprint_script'::text AS geomsource
    FROM {{ ref('dcp_id_bin_map') }} AS m
    INNER JOIN {{ ref('stg__doitt_buildingfootprints') }} AS b ON m.bin::bigint = b.bin::bigint
    WHERE b.wkb_geometry IS NOT NULL
),

manual_geoms_2020 AS (
    SELECT
        maprojid,
        ST_SETSRID(ST_GEOMFROMTEXT(geometry), 4326) AS geom,
        CASE
            WHEN footprint_project_geomsource ~* 'AD Sprint' THEN 'EP 2020'
            ELSE footprint_project_geomsource
        END AS geomsource
    FROM {{ ref('dcp_manual_geoms_2020') }}
    WHERE geometry IS NOT NULL
),

dot_bridges AS (
    SELECT
        REGEXP_REPLACE(fms_id, ' ', '') AS maprojid,
        wkb_geometry::geometry AS geom
    FROM {{ source('cpdb_legacy_pipeline', 'dot_projects_bridges_byfms') }}
),

dot_intersections AS (
    SELECT
        fmsagencyid || fmsid AS maprojid,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__dot_projects_intersections') }}
    GROUP BY fmsagencyid, fmsid
),

dot_streets AS (
    SELECT
        fmsagencyid || fmsid AS maprojid,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__dot_projects_streets') }}
    GROUP BY fmsagencyid, fmsid
),

dot AS (
    SELECT
        base.maprojid,
        COALESCE(streets.geom, intersections.geom, bridges.geom) AS geom,
        'dot'::text AS geomsource,
        'dot'::text AS datasource,
        CASE
            WHEN streets.geom IS NOT NULL THEN 'dot_projects_streets'
            WHEN intersections.geom IS NOT NULL THEN 'dot_projects_intersections'
            WHEN bridges.geom IS NOT NULL THEN 'dot_projects_bridges'
        END AS dataname
    FROM base
    LEFT JOIN dot_bridges AS bridges ON base.maprojid = bridges.maprojid
    LEFT JOIN dot_intersections AS intersections ON base.maprojid = intersections.maprojid
    LEFT JOIN dot_streets AS streets ON base.maprojid = streets.maprojid
    WHERE bridges.geom IS NOT NULL OR intersections.geom IS NOT NULL OR streets.geom IS NOT NULL
),

dpr_fmsid AS (
    SELECT
        REPLACE(fmsid, ' ', '') AS maprojid,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom,
        'dpr'::text AS geomsource,
        'dpr'::text AS datasource,
        'dpr_capitalprojects'::text AS dataname
    FROM {{ ref('stg__dpr_capitalprojects') }}
    WHERE NOT COALESCE(lat = 0 OR lon = 0, FALSE)
    GROUP BY REPLACE(fmsid, ' ', '')
    HAVING ST_MULTI(ST_UNION(wkb_geometry)) IS NOT NULL
),

edc_capitalprojects AS (
    SELECT
        REPLACE(pjid, ' ', '') AS maprojid,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__edc_capitalprojects') }}
    GROUP BY pjid
),

edc_ferry AS (
    SELECT
        fmsid AS maprojid,
        wkb_geometry AS geom
    FROM {{ ref('stg__edc_capitalprojects_ferry') }}
),

edc AS (
    SELECT
        base.maprojid,
        COALESCE(ferry.geom, capitalprojects.geom) AS geom,
        'edc'::text AS geomsource,
        'edc'::text AS datasource,
        CASE
            WHEN ferry.geom IS NOT NULL THEN 'edc_capitalprojects_ferry'
            WHEN capitalprojects.geom IS NOT NULL THEN 'edc_capitalprojects'
        END AS dataname
    FROM base
    LEFT JOIN edc_capitalprojects AS capitalprojects ON base.maprojid = capitalprojects.maprojid
    LEFT JOIN edc_ferry AS ferry ON base.maprojid = ferry.maprojid
    WHERE capitalprojects.geom IS NOT NULL OR ferry.geom IS NOT NULL
),

ddc_infrastructure AS (
    SELECT
        '850' || projectid AS maprojid,
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom
    FROM {{ ref('stg__ddc_capitalprojects_infrastructure') }}
    GROUP BY projectid
),

ddc_buildings AS (
    SELECT
        '850' || projectid AS maprojid,
        ST_MULTI(ST_UNION(ST_CENTROID(wkb_geometry))) AS geom
    FROM {{ ref('stg__ddc_capitalprojects_publicbuildings') }}
    GROUP BY projectid
),

ddc AS (
    SELECT
        base.maprojid,
        COALESCE(buildings.geom, infrastructure.geom) AS geom,
        'ddc'::text AS geomsource,
        'ddc'::text AS datasource,
        CASE
            WHEN buildings.geom IS NOT NULL THEN 'ddc_capitalprojects_publicbuildings'
            WHEN infrastructure.geom IS NOT NULL THEN 'ddc_capitalprojects_infrastructure'
        END AS dataname
    FROM base
    LEFT JOIN ddc_infrastructure AS infrastructure ON base.maprojid = infrastructure.maprojid
    LEFT JOIN ddc_buildings AS buildings ON base.maprojid = buildings.maprojid
    WHERE infrastructure.geom IS NOT NULL OR buildings.geom IS NOT NULL
)

SELECT
    base.ccpversion,
    base.maprojid,
    base.magency,
    base.magencyacro,
    base.projectid,
    base.description,
    base.typecategory,
    COALESCE(
        ddc.geomsource, edc.geomsource, dpr_fmsid.geomsource, dot.geomsource,
        manual_geoms_2020.geomsource, id_bin_map.geomsource, dcp_json.geomsource, sprints.geomsource
    ) AS geomsource,
    COALESCE(ddc.dataname, edc.dataname, dpr_fmsid.dataname, dot.dataname) AS dataname,
    COALESCE(ddc.datasource, edc.datasource, dpr_fmsid.datasource, dot.datasource) AS datasource,
    base.datadate,
    COALESCE(
        ddc.geom, edc.geom, dpr_fmsid.geom, dot.geom,
        manual_geoms_2020.geom, id_bin_map.geom, dcp_json.geom, sprints.geom
    ) AS geom
FROM base
LEFT JOIN sprints ON base.maprojid = sprints.maprojid
LEFT JOIN dcp_json ON base.maprojid = dcp_json.maprojid
LEFT JOIN id_bin_map ON base.maprojid = id_bin_map.maprojid
LEFT JOIN manual_geoms_2020 ON base.maprojid = manual_geoms_2020.maprojid
LEFT JOIN dot ON base.maprojid = dot.maprojid
LEFT JOIN dpr_fmsid ON base.maprojid = dpr_fmsid.maprojid
LEFT JOIN edc ON base.maprojid = edc.maprojid
LEFT JOIN ddc ON base.maprojid = ddc.maprojid
