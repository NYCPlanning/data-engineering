-- stg__pops.sql

{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['raw_geom'], 'type': 'gist'},
    ]
) }}

WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dcp_pops') }}
),

final AS (
    SELECT
        'pops' AS variable_type,
        pops_number AS variable_id,
        bbl::text,
        ST_Transform(wkb_geometry, 2263) AS raw_geom
    FROM source

)

SELECT * FROM final
