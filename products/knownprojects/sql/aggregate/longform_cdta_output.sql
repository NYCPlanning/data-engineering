{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="""
        UPDATE {{ this }} AS a
        SET
            cdta = b.cdta2020,
            proportion_in_cdta = 1,
            units_net_in_cdta = a.units_net
        FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.dcp_cdta2020 AS b
        WHERE
            a.cdta IS NULL
            AND NOT st_isempty(a.geometry)
            AND st_intersects(a.geometry, b.geometry)
    """
) }}

-- Project records allocated to Community District Tabulation Areas, one row per project/cdta pair.
-- The post-hook assigns any project that matched no cdta to one it
-- intersects; it is arbitrary when several intersect, carried over from the
-- source script's closing UPDATE.

{{ longform_by_boundary(
    boundary_table='dcp_cdta2020',
    boundary_cols=[{'source': 'cdta2020', 'alias': 'cdta'}],
    suffix='cdta'
) }}
