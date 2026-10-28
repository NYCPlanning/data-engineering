SELECT
    borocd,
    units_2020 AS housing_units_2020_end,
    completed_units_2015_2024 AS net_completed_units_2015_2024,
    completed_units_2021_2024 AS net_completed_units_2021_2024,
    units_2024 AS housing_units_2024_end,
    projected_completed_units_2025_2034 AS projected_net_units_2025_2034,
    projected_units_2034 AS projected_housing_units_2034_end
FROM {{ ref('stg_kpdb__cpp_housing_growth_cd') }}
