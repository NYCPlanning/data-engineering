{{ config(materialized='table', tags=['qa']) }}

-- Runs test_cases.geometry_diff_metrics (macros/create_pg_functions.sql) against every
-- row in test_cases.examples - our TDD fixture table for known-weird geometries (see
-- products/cscl chat log, 2026-10-04). A row "passes" when none of its metrics exceed
-- their threshold; data_tests in _etl_dev_qa.yml asserts every row passes, same as any
-- other test suite - this model is expected to go red when a new weird case is added
-- (or an existing fix regresses) and green once the underlying geometry issue is
-- actually fixed, not a one-time snapshot.
--
-- Thresholds are dbt vars (override with --vars rather than editing this file), picked
-- from exactly two data points so far (chat log 2026-10-04) - expect to retune as more
-- examples land across layers of different scale:
--   symdiff_area_pct        1%    - generous; real hairline/overlay noise should be
--                                    far below this, a genuinely missing/extra chunk of
--                                    land should be far above it.
--   hausdorff_ft / symdiff_boundary_len_ft  50 / 100 ft - the one real example measured
--                                    (337.57 / 770.31) is far past either; small enough
--                                    that minor coastline noise on a huge borough-scale
--                                    feature could plausibly need a looser, layer-aware
--                                    threshold later.
--   max_ring_eccentricity   50    - compact real polygons measured in the single digits
--                                    to low tens so far; the needle-sliver test case
--                                    measured 2885.76, two orders of magnitude past this.
--   perimeter_pct_diff       1%   - raw, unfiltered SHAPE_Length comparison, added
--                                    2026-10-05 because symdiff_area_pct/
--                                    symdiff_boundary_len_ft only sum fragments >=
--                                    min_fragment_sqft (100 sqft default) and every
--                                    needle-sliver fragment found so far is under that -
--                                    those two metrics are structurally blind to this
--                                    defect class at production thresholds. Confirmed on
--                                    PUMA 3701 (chat log 2026-10-05): symdiff_area_pct=0,
--                                    symdiff_fragment_count=0, but perimeter_pct_diff=22%
--                                    (matches the real SHAPE_Length gap vs prod exactly).
-- A metric that's NULL (no expected_geom to compare against) is never treated as a
-- failure on its own - see geometry_diff_metrics' docstring for when that happens.

WITH examples AS (
    SELECT * FROM {{ adapter.get_relation(
        database="db-cscl", schema="test_cases", identifier="examples"
    ) }}
),

metrics AS (
    SELECT
        e.slug,
        e.label,
        e.category,
        e.layer,
        e.key_column,
        e.key_value,
        e.status,
        m.actual_area_sqft,
        m.expected_area_sqft,
        m.symdiff_area_pct,
        m.hausdorff_ft,
        m.symdiff_boundary_len_ft,
        m.max_ring_eccentricity,
        m.perimeter_pct_diff
    FROM examples AS e
    CROSS JOIN LATERAL test_cases.geometry_diff_metrics(e.actual_geom, e.expected_geom) AS m
),

flagged AS (
    SELECT
        *,
        array_remove(ARRAY[
            CASE
                WHEN symdiff_area_pct > {{ var('geom_check_symdiff_area_pct', 1) }}
                    THEN 'symdiff_area_pct'
            END,
            CASE
                WHEN hausdorff_ft > {{ var('geom_check_hausdorff_ft', 50) }}
                    THEN 'hausdorff_ft'
            END,
            CASE
                WHEN symdiff_boundary_len_ft > {{ var('geom_check_symdiff_boundary_len_ft', 100) }}
                    THEN 'symdiff_boundary_len_ft'
            END,
            CASE
                WHEN max_ring_eccentricity > {{ var('geom_check_max_ring_eccentricity', 50) }}
                    THEN 'max_ring_eccentricity'
            END,
            CASE
                WHEN abs(perimeter_pct_diff) > {{ var('geom_check_perimeter_pct', 1) }}
                    THEN 'perimeter_pct_diff'
            END
        ], NULL) AS failing_metrics
    FROM metrics
)

SELECT
    *,
    (array_length(failing_metrics, 1) IS NULL) AS passes
FROM flagged
