{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="{{ boundary_fallback_update(boundary_table='dcp_cdboundaries_wi', id_column='borocd', suffix='cd') }}"
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
