SELECT
    geography_id::integer AS borocd,
    units_2020_census,
    units_2020,
    completed_units_2015_2024,
    completed_units_2021_2024,
    units_2024,
    projected_completed_units_2025_2034,
    projected_units_2034
FROM {{ source('sources', 'kpdb_cpp_housing_growth_cd') }}
