{{ config(materialized='table', tags=['qa']) }}

-- Every district that isn't identical to prod's, worst first: one-sided and
-- duplicate_key districts (no symdiff_area_pct), then by symdiff_area_pct. The district
-- review FGDB's summary table.

SELECT
    row_number() OVER (
        ORDER BY symdiff_area_pct DESC NULLS FIRST, hausdorff_ft DESC, layer ASC, district_key ASC
    )::INTEGER AS rank,
    *
FROM {{ ref('qa__district_review_districts') }}
WHERE class != 'identical'
