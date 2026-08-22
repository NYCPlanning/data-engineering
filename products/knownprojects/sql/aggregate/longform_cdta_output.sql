{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="{{ boundary_fallback_update(boundary_table='dcp_cdta2020', id_column='cdta2020', suffix='cdta') }}"
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
