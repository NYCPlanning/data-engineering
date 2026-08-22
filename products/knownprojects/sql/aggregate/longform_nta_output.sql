{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="{{ boundary_fallback_update(boundary_table='dcp_nta2020', id_column='nta2020', suffix='nta') }}"
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
