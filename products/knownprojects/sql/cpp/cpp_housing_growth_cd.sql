{{ config(materialized='table', tags=['cpp']) }}

-- Housing growth by community district, for the Capital Projects Portal.

{{ cpp_housing_growth(
    geography_type='CD',
    kpdb_model='future_units_by_cd',
    kpdb_geo_column='cd',
    housing_column='comunitydist'
) }}
