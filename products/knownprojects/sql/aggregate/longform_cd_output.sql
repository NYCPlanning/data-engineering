{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="""
        UPDATE {{ this }} AS a
        SET
            cd = b.borocd,
            proportion_in_cd = 1,
            units_net_in_cd = a.units_net
        FROM {{ source('recipe_sources', 'dcp_cdboundaries_wi') }} AS b
        WHERE
            a.cd IS NULL
            AND NOT st_isempty(a.geometry)
            AND st_intersects(a.geometry, b.geometry)
    """
) }}

-- Project records allocated to community districts, one row per project/cd pair.
-- The post-hook assigns any project that matched no cd to one it
-- intersects; it is arbitrary when several intersect, carried over from the
-- source script's closing UPDATE.

{{ longform_by_boundary(
    boundary_table='dcp_cdboundaries_wi',
    boundary_cols=[{'source': 'borocd', 'alias': 'cd'}],
    suffix='cd'
) }}
