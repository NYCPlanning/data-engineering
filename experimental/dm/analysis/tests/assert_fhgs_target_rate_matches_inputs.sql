-- The report's target rate is the baseline rate times each adjustment (LAA x 1.25, transit
-- x 1.01-1.1, tidal flooding x 0.8-0.95). The report prints baselines rounded to 2 decimals
-- before compounding them, so recomputed rates differ by up to about 0.01. A row here is a
-- bigger difference than that, which means an extraction error.
SELECT
    borocd,
    target_growth_rate_pct,
    baseline_target_growth_rate_pct
    * (1 + coalesce(laa_adjustment_pct, 0) / 100)
    * (1 + coalesce(transit_access_adjustment_pct, 0) / 100)
    * (1 - coalesce(tidal_flooding_adjustment_pct, 0) / 100) AS recomputed_target_growth_rate_pct
FROM {{ ref('stg_fhgs__target_inputs') }}
WHERE abs(
    target_growth_rate_pct
    - baseline_target_growth_rate_pct
    * (1 + coalesce(laa_adjustment_pct, 0) / 100)
    * (1 + coalesce(transit_access_adjustment_pct, 0) / 100)
    * (1 - coalesce(tidal_flooding_adjustment_pct, 0) / 100)
) > 0.015
