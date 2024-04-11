{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH mappluto_wi AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_mappluto_wi') }}
),

final AS (
    SELECT
        bbl::text,
        zonedist1,
        zonedist2,
        zonedist3,
        zonedist4,
        spdist1,
        spdist2,
        spdist3,
        landuse,
        ST_TRANSFORM(wkb_geometry, 2263) AS geom
    FROM mappluto_wi
)

SELECT * FROM final
