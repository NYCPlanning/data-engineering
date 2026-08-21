{{ config(materialized='table', tags=['cpp']) }}

-- Housing growth by Neighborhood Tabulation Area, for the Capital Projects Portal.

{{ cpp_housing_growth(
    geography_type='NTA2020',
    kpdb_model='future_units_by_nta',
    kpdb_geo_column='nta',
    housing_column='nta2020'
) }}
