{{
    config(
        materialized='table',
        indexes=[{'columns': ['tax_sub_block_id'], 'unique': True}],
        tags=['blocks']
    )
}}

SELECT
    block_id AS tax_sub_block_id,
    borough,
    block,
    sub_block,
    bbls,
    geom
FROM {{ ref('int_tz__tax_blocks') }}
