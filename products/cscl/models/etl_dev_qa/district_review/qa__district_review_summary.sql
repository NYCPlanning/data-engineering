{{ config(materialized='table', tags=['qa']) }}

-- Every district that isn't identical to prod's, worst first: one-sided districts, then
-- by symdiff_area_pct. The district review FGDB's summary table.

SELECT
    row_number() OVER (
        ORDER BY symdiff_area_pct DESC NULLS FIRST, hausdorff_ft DESC, layer, district_key
    ) AS rank,
    * EXCLUDE (geom_new, geom_prior)
FROM {{ ref('qa__district_review_districts') }}
WHERE class != 'identical'
