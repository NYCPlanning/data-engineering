SELECT
    spine.* EXCLUDE (geom),
    types.nta_type_name,
    census.* EXCLUDE (nta2020),
    acs.* EXCLUDE (nta2020),
    density.* EXCLUDE (nta2020),
    housing.housing_units_2024_end,
    kpdb.projected_net_units_2025_2034,
    kpdb.projected_net_units_2025_2029,
    kpdb.projected_net_units_2030_2034,
    housing.projected_housing_units_2034_end,
    spine.geom
FROM {{ ref('int_nta_spine') }} AS spine
LEFT JOIN {{ ref('int_nta_type_features') }} AS types ON spine.nta2020 = types.nta2020
LEFT JOIN {{ ref('int_nta_census_metrics') }} AS census ON spine.nta2020 = census.nta2020
LEFT JOIN {{ ref('int_nta_acs_metrics') }} AS acs ON spine.nta2020 = acs.nta2020
LEFT JOIN {{ ref('int_nta_density_metrics') }} AS density ON spine.nta2020 = density.nta2020
LEFT JOIN {{ ref('int_nta_housing_metrics') }} AS housing ON spine.nta2020 = housing.nta2020
LEFT JOIN {{ ref('int_nta_kpdb_projection_metrics') }} AS kpdb ON spine.nta2020 = kpdb.nta2020
ORDER BY spine.nta2020
