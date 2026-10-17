-- each project's borough and community district, from its geometry when that intersects
-- one, else from the Capital Projects Dashboard
WITH geoms AS (
    SELECT
        maprojid,
        geom
    FROM {{ ref('cpdb_dcpattributes') }}
    WHERE geom IS NOT NULL
),

geom_boroughs AS (
    SELECT
        g.maprojid,
        ARRAY_AGG(DISTINCT b.boroname ORDER BY b.boroname) AS boroughs
    FROM geoms AS g
    INNER JOIN {{ ref('stg__dcp_boroboundaries_wi') }} AS b
        ON ST_INTERSECTS(g.geom, b.wkb_geometry)
    GROUP BY g.maprojid
),

geom_community_districts AS (
    SELECT
        g.maprojid,
        ARRAY_AGG(DISTINCT b.borocd::text ORDER BY b.borocd::text) AS community_districts
    FROM geoms AS g
    INNER JOIN {{ ref('stg__dcp_cdboundaries') }} AS b
        ON ST_INTERSECTS(g.geom, b.wkb_geometry)
    GROUP BY g.maprojid
),

dashboard AS (
    SELECT
        maprojid,
        is_citywide,
        boroughs,
        -- a single community district only counts when every agency project names it
        CASE
            WHEN CARDINALITY(community_districts) > 1 THEN community_districts
            WHEN every_row_has_community_district THEN community_districts
        END AS community_districts
    FROM {{ ref('int__project_dashboard_areas') }}
),

-- 'multiple' when a project is in more than one area
labeled AS (
    SELECT
        p.maprojid,
        CASE
            WHEN CARDINALITY(gb.boroughs) = 1 THEN gb.boroughs[1]
            WHEN CARDINALITY(gb.boroughs) > 1 THEN 'multiple'
        END AS geom_borough,
        CASE
            WHEN CARDINALITY(d.boroughs) = 1 THEN d.boroughs[1]
            WHEN CARDINALITY(d.boroughs) > 1 THEN 'multiple'
        END AS dashboard_borough,
        CASE
            WHEN CARDINALITY(gc.community_districts) = 1 THEN gc.community_districts[1]
            WHEN CARDINALITY(gc.community_districts) > 1 THEN 'multiple'
        END AS geom_community_district,
        CASE
            WHEN CARDINALITY(d.community_districts) = 1 THEN d.community_districts[1]
            WHEN CARDINALITY(d.community_districts) > 1 THEN 'multiple'
        END AS dashboard_community_district,
        COALESCE(gc.community_districts, d.community_districts) AS community_districts,
        COALESCE(d.is_citywide, FALSE) AS dashboard_citywide
    FROM {{ ref('int__ccp_projects') }} AS p
    LEFT JOIN geom_boroughs AS gb ON p.maprojid = gb.maprojid
    LEFT JOIN geom_community_districts AS gc ON p.maprojid = gc.maprojid
    LEFT JOIN dashboard AS d ON p.maprojid = d.maprojid
)

SELECT
    maprojid,
    COALESCE(geom_borough, dashboard_borough) AS borough,
    CASE
        WHEN geom_borough IS NOT NULL THEN 'geometry'
        WHEN dashboard_borough IS NOT NULL THEN 'dashboard'
    END AS borough_source,
    COALESCE(geom_community_district, dashboard_community_district) AS community_district,
    CASE
        WHEN geom_community_district IS NOT NULL THEN 'geometry'
        WHEN dashboard_community_district IS NOT NULL THEN 'dashboard'
    END AS community_district_source,
    ARRAY_TO_STRING(community_districts, ', ') AS community_districts,
    dashboard_citywide
FROM labeled
