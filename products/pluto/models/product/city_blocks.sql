{{
    config(
        materialized='table',
        indexes=[{'columns': ['city_block_id'], 'unique': True}],
        tags=['blocks']
    )
}}

SELECT
    city_block_id,
    is_mostly_water,
    tax_block_count,
    has_multiple_tax_blocks,
    water_share,
    bbls,
    geom
FROM {{ ref('int_blocks__city_blocks') }}
