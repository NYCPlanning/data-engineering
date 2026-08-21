{{ config(
    materialized='table',
    tags=['aggregate_sca'],
    post_hook="""
        UPDATE {{ this }} AS a
        SET
            csd = b.schooldist,
            proportion_in_csd = 1,
            units_net_in_csd = a.units_net
        FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.dcp_school_districts AS b
        WHERE
            a.csd IS NULL
            AND NOT st_isempty(a.geometry)
            AND st_intersects(a.geometry, b.geometry)
    """
) }}

-- Project records allocated to community school districts.
-- The post-hook assigns any project that matched no district to one it
-- intersects; it is arbitrary when several intersect, carried over from the
-- source script's closing UPDATE.

{{ longform_by_boundary(
    boundary_table='dcp_school_districts',
    boundary_cols=[{'source': 'schooldist', 'alias': 'csd'}],
    suffix='csd'
) }}
