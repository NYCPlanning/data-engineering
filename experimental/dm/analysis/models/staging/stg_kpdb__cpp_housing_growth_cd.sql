SELECT
    geography_id::integer AS borocd,
    units_2020_census,
    projected_completed_units_2025_2034
FROM {{ source('sources', 'kpdb_cpp_housing_growth_cd') }}
