SELECT
    spine.* EXCLUDE (geom),
    census.* EXCLUDE (boroct2020),
    acs.* EXCLUDE (boroct2020),
    density.* EXCLUDE (boroct2020),
    spine.geom
FROM {{ ref('int_census_tract_spine') }} AS spine
LEFT JOIN {{ ref('int_census_tract_census_metrics') }} AS census ON spine.boroct2020 = census.boroct2020
LEFT JOIN {{ ref('int_census_tract_acs_metrics') }} AS acs ON spine.boroct2020 = acs.boroct2020
LEFT JOIN {{ ref('int_census_tract_density_metrics') }} AS density ON spine.boroct2020 = density.boroct2020
ORDER BY spine.boroct2020
