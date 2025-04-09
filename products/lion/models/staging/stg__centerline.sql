{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']}
    ]
) }}

SELECT * FROM {{ source("recipe_sources", "dcp_cscl_centerline") }}
