{{ config(materialized='table', tags=['aggregate_sca']) }}

-- Project records allocated to school subdistricts.
-- The source script's fallback UPDATE was commented out as fixing no records,
-- so there is deliberately no post-hook here.

{{ longform_by_boundary(
    boundary_table='doe_school_subdistricts',
    boundary_cols=[
        {'source': 'district', 'alias': 'distzone'},
        {'source': 'subdistrict', 'alias': 'subdistzone'},
        {'source': 'name', 'alias': 'a_dist_zone_name'}
    ],
    suffix='subdist'
) }}
