SELECT
    geography_id AS nta2020,
    units_2020_census,
    projected_completed_units_2025_2034
FROM {{ source('sources', 'kpdb_cpp_housing_growth_nta') }}
