SELECT
    spine.* EXCLUDE (geom),
    types.nta_type_name,
    census.* EXCLUDE (nta2020),
    acs.* EXCLUDE (nta2020),
    density.* EXCLUDE (nta2020),
    spine.geom
FROM {{ ref('int_nta_spine') }} AS spine
LEFT JOIN {{ ref('int_nta_type_features') }} AS types ON spine.nta2020 = types.nta2020
LEFT JOIN {{ ref('int_nta_census_metrics') }} AS census ON spine.nta2020 = census.nta2020
LEFT JOIN {{ ref('int_nta_acs_metrics') }} AS acs ON spine.nta2020 = acs.nta2020
LEFT JOIN {{ ref('int_nta_density_metrics') }} AS density ON spine.nta2020 = density.nta2020
ORDER BY spine.nta2020
