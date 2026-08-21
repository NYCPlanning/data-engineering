{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="""
        UPDATE {{ this }} AS a
        SET
            ct = b.boroct2020,
            proportion_in_ct = 1,
            units_net_in_ct = a.units_net
        FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.dcp_ct2020_wi AS b
        WHERE
            a.ct IS NULL
            AND NOT st_isempty(a.geometry)
            AND st_intersects(a.geometry, b.geometry)
    """
) }}

-- Project records allocated to census tracts, one row per project/ct pair.
-- The post-hook assigns any project that matched no ct to one it
-- intersects; it is arbitrary when several intersect, carried over from the
-- source script's closing UPDATE.

{{ longform_by_boundary(
    boundary_table='dcp_ct2020_wi',
    boundary_cols=[{'source': 'boroct2020', 'alias': 'ct'}],
    suffix='ct'
) }}
