{{ config(materialized='table', tags=['qa']) }}

-- One row per (layer, district): discrepancy metrics and a class. The _new, _prior and
-- summary layers of the district review FGDB are cuts of this table, joined to
-- qa__district_review_pairs for the geometry (not copied here: both sides of every
-- census block twice over is over a GB).
--
-- Metrics count only symmetric-difference pieces above grid noise (see
-- qa__district_review_pieces). symdiff_area_pct is over our area, as in
-- geometry_diff_metrics. hausdorff_ft comes from geometry_diff_metrics
-- (macros/geometry_diff_qa.sql), run only on districts with a piece above noise: exact
-- above geom_check_hausdorff_tolerance_ft, 0 below it.
--
-- class:
--   new_only / prior_only - the key is on one side only
--   duplicate_key - the key is on both sides, as a different number of features
--                (district_review_is_duplicate_key): no metrics, n_features_new and
--                n_features_prior say how many
--   identical  - no symmetric difference at all
--   grid_noise - every piece is under district_review_noise_width_ft wide
--   major      - symdiff_area_pct > geom_check_symdiff_area_pct or
--                hausdorff_ft > geom_check_hausdorff_ft (the thresholds
--                geometry_diff_qa flags on)
--   minor      - anything else
--
-- known_difference_id: NULL until the known-differences seed exists to map from.

WITH all_pairs AS (
    SELECT
        *,
        {{ district_review_is_duplicate_key() }} AS is_duplicate_key
    FROM {{ ref('qa__district_review_pairs') }}
),

pairs AS (
    SELECT * FROM all_pairs
    WHERE NOT is_duplicate_key
),

pieces AS (
    SELECT
        layer,
        district_key,
        count(*) AS n_pieces,
        (count(*) FILTER (WHERE NOT is_noise))::INTEGER AS symdiff_fragment_count,
        coalesce(sum(area_sqft) FILTER (WHERE NOT is_noise), 0) AS symdiff_area,
        coalesce(sum(perimeter_ft) FILTER (WHERE NOT is_noise), 0) AS symdiff_boundary_len_ft
    FROM {{ ref('qa__district_review_pieces') }}
    GROUP BY layer, district_key
),

hd_input AS (
    SELECT
        p.layer,
        p.district_key,
        st_makevalid(p.geom_new) AS a,
        p.geom_prior AS e
    FROM pairs AS p
    INNER JOIN pieces AS pc ON p.layer = pc.layer AND p.district_key = pc.district_key
    WHERE pc.symdiff_fragment_count > 0
),

hd AS (
    SELECT
        layer,
        district_key,
        hausdorff_ft
    FROM ({{ geometry_diff_metrics('hd_input', 'a', 'e') }}) AS m
),

measured AS (
    SELECT
        p.layer,
        p.key_name,
        p.district_key,
        p.n_features_new,
        p.n_features_prior,
        st_area(p.geom_new) AS area_new,
        st_area(p.geom_prior) AS area_prior,
        st_area(p.geom_new) - st_area(p.geom_prior) AS area_delta,
        (st_area(p.geom_new) - st_area(p.geom_prior)) / nullif(st_area(p.geom_prior), 0) * 100 AS area_delta_pct,
        CASE WHEN p.geom_new IS NOT NULL AND p.geom_prior IS NOT NULL THEN coalesce(pc.symdiff_area, 0) END
            AS symdiff_area,
        CASE
            WHEN p.geom_new IS NOT NULL AND p.geom_prior IS NOT NULL
                THEN coalesce(pc.symdiff_area, 0) / nullif(st_area(p.geom_new), 0) * 100
        END AS symdiff_area_pct,
        CASE WHEN p.geom_new IS NOT NULL AND p.geom_prior IS NOT NULL THEN coalesce(hd.hausdorff_ft, 0) END
            AS hausdorff_ft,
        CASE WHEN p.geom_new IS NOT NULL AND p.geom_prior IS NOT NULL THEN coalesce(pc.symdiff_boundary_len_ft, 0) END
            AS symdiff_boundary_len_ft,
        CASE WHEN p.geom_new IS NOT NULL AND p.geom_prior IS NOT NULL THEN coalesce(pc.symdiff_fragment_count, 0) END
            AS symdiff_fragment_count,
        coalesce(pc.n_pieces, 0) AS n_pieces,
        p.is_equal,
        p.geom_new,
        p.geom_prior
    FROM pairs AS p
    LEFT JOIN pieces AS pc ON p.layer = pc.layer AND p.district_key = pc.district_key
    LEFT JOIN hd ON p.layer = hd.layer AND p.district_key = hd.district_key
)

SELECT
    layer,
    key_name,
    district_key,
    n_features_new,
    n_features_prior,
    area_new,
    area_prior,
    area_delta,
    area_delta_pct,
    symdiff_area,
    symdiff_area_pct,
    hausdorff_ft,
    symdiff_boundary_len_ft,
    symdiff_fragment_count,
    CASE
        WHEN geom_prior IS NULL THEN 'new_only'
        WHEN geom_new IS NULL THEN 'prior_only'
        WHEN is_equal OR n_pieces = 0 THEN 'identical'
        WHEN symdiff_fragment_count = 0 THEN 'grid_noise'
        WHEN
            symdiff_area_pct > {{ var('geom_check_symdiff_area_pct', 1) }}
            OR hausdorff_ft > {{ var('geom_check_hausdorff_ft', 50) }}
            THEN 'major'
        ELSE 'minor'
    END AS class,
    NULL::TEXT AS known_difference_id
FROM measured

UNION ALL BY NAME

SELECT
    layer,
    key_name,
    district_key,
    n_features_new,
    n_features_prior,
    'duplicate_key' AS class,
    NULL::TEXT AS known_difference_id
FROM all_pairs
WHERE is_duplicate_key
