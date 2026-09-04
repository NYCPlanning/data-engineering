-- stg__pops.sql

{{ config(
    materialized = 'table'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dcp_pops') }}
),

final AS (
    SELECT
        'pops' AS variable_type,
        pops_number AS variable_id,
        bbl::text AS bbl,
        {{ dcp_st_transform(dcp_geom_column('dcp_pops'), 2263) }} AS raw_geom
    FROM source

)

SELECT * FROM final
