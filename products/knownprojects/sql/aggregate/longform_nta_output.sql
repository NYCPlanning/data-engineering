{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="""
        UPDATE {{ this }} AS a
        SET
            nta = b.nta2020,
            proportion_in_nta = 1,
            units_net_in_nta = a.units_net
        FROM {{ source('recipe_sources', 'dcp_nta2020') }} AS b
        WHERE
            a.nta IS NULL
            AND NOT st_isempty(a.geometry)
            AND st_intersects(a.geometry, b.geometry)
    """
) }}

-- Project records allocated to Neighborhood Tabulation Areas, one row per project/nta pair.
-- The post-hook assigns any project that matched no nta to one it
-- intersects; it is arbitrary when several intersect, carried over from the
-- source script's closing UPDATE.

{{ longform_by_boundary(
    boundary_table='dcp_nta2020',
    boundary_cols=[{'source': 'nta2020', 'alias': 'nta'}],
    suffix='nta'
) }}
