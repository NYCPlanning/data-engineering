SELECT
    {{ borocd_from_cd_code('inputs.community_district') }} AS borocd,
    inputs.community_district_name,
    inputs.growth_cohort,
    rates.annual_growth_rate_2016_2025_pct,
    inputs.baseline_target_growth_rate_pct,
    inputs.laa_adjustment_pct,
    inputs.transit_access_adjustment_pct,
    inputs.tidal_flooding_adjustment_pct,
    inputs.target_growth_rate_pct,
    inputs.total_homes_2025
FROM {{ ref('fhgs_2030_target_inputs') }} AS inputs
LEFT JOIN {{ ref('fhgs_growth_rates_2016_2025') }} AS rates
    ON inputs.community_district = rates.community_district
