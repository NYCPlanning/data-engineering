{{ config(materialized='table', tags=['qa']) }}

-- One row per district layer: district counts per class and the worst district. The
-- district review FGDB's summary_by_layer table.

SELECT
    layer,
    count(*) FILTER (WHERE class != 'prior_only') AS n_new,
    count(*) FILTER (WHERE class != 'new_only') AS n_prior,
    count(*) FILTER (WHERE class = 'identical') AS n_identical,
    count(*) FILTER (WHERE class = 'grid_noise') AS n_grid_noise,
    count(*) FILTER (WHERE class = 'minor') AS n_minor,
    count(*) FILTER (WHERE class = 'major') AS n_major,
    count(*) FILTER (WHERE class = 'new_only') AS n_new_only,
    count(*) FILTER (WHERE class = 'prior_only') AS n_prior_only,
    coalesce(sum(symdiff_area), 0) AS symdiff_area,
    max(symdiff_area_pct) AS max_symdiff_area_pct,
    arg_max(district_key, symdiff_area_pct) AS max_symdiff_area_pct_district,
    max(hausdorff_ft) AS max_hausdorff_ft
FROM {{ ref('qa__district_review_districts') }}
GROUP BY layer
ORDER BY layer
