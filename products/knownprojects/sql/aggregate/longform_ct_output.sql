{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="{{ boundary_fallback_update(boundary_table='dcp_ct2020_wi', id_column='boroct2020', suffix='ct') }}"
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
