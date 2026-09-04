{{ config(
    materialized = 'table',
    post_hook="CREATE INDEX IF NOT EXISTS stg__pluto_geom_idx ON {{ this }} USING RTREE (geom)"
) }}

WITH mappluto_wi AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_mappluto_wi') }}
),

final AS (
    SELECT
        bbl::text AS bbl,
        zonedist1,
        zonedist2,
        zonedist3,
        zonedist4,
        spdist1,
        spdist2,
        spdist3,
        landuse,
        {{ dcp_st_transform(dcp_geom_column('dcp_mappluto_wi'), 2263) }} AS geom
    FROM mappluto_wi
)

SELECT * FROM final
