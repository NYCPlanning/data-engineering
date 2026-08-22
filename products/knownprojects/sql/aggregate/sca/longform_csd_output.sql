{{ config(
    materialized='table',
    tags=['aggregate_sca'],
    post_hook="{{ boundary_fallback_update(boundary_table='dcp_school_districts', id_column='schooldist', suffix='csd') }}"
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
