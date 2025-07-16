WITH projects AS (
    SELECT * FROM {{ ref("stg__zap_projects") }}
),

pluto AS (
    SELECT
        bbl,
        wkb_geometry
    FROM {{ source("recipe_sources", "dcp_mappluto_clipped") }}
),

project_bbls AS (
    SELECT DISTINCT
        project_id,
        trim(unnest(string_to_array(bbls, ','))) AS bbl
    FROM projects
    WHERE bbls IS NOT NULL
),

zap_lots AS (
    SELECT
        project_bbls.project_id,
        project_bbls.bbl,
        pluto.bbl IS NOT NULL AS joined_to_pluto,
        ST_Transform(pluto.wkb_geometry, 2263) AS geom
    FROM project_bbls
    LEFT JOIN pluto
        ON project_bbls.bbl = pluto.bbl::text
)

SELECT * FROM zap_lots
