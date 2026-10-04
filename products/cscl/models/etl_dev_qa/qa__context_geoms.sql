{{ config(materialized='table', tags=['qa']) }}

-- Every displayable geometry for a flagged row, one normalized table - the PRIMARY
-- pair (our own build output, and the legacy/prod geometry it's compared against)
-- plus any RELATED-but-distinct geometry that helps explain why the row was flagged
-- (e.g. a PUMA's member census tracts, when investigating a tract-dissolve
-- needle-sliver). The primary pair also lives as actual_geom_4326/expected_geom_4326
-- columns directly on qa__geometry_fgdb_* - duplicated here too (not instead of) so a
-- future map UI (or anything else consuming this) can query ONE table
-- (layer, key_value, role) for everything about a flagged feature, rather than
-- switching schemas between "the row itself" and "things related to it"
-- (chat log 2026-10-06). NOT part of qa__diffs_all's lightweight summary rollup.
--
-- role values so far: 'build' (our own output), 'legacy' (the prod/legacy geometry
-- it's compared against), 'constituent_tract' (a PUMA's member tracts), and four
-- roles splitting ST_Difference of build vs legacy into REAL defects vs noise (chat
-- log 2026-10-06, see below for why that split exists and how it's computed):
--   'build_spur'    - real area in build, not in legacy (e.g. a self-touching-vertex
--                     artifact exposed by clip_to_shoreline)
--   'legacy_spur'   - real area in legacy, not in build
--   'build_sliver'  - noise: area in build not in legacy, but within
--                     context_geom_noise_buffer_ft of the true boundary (tract-dissolve
--                     needle retrace, not a real defect)
--   'legacy_sliver' - noise, same idea, area in legacy not in build
-- build_spur + build_sliver together reconstruct the full raw ST_Difference(build,
-- legacy) (same for legacy_spur + legacy_sliver) - the split is exhaustive, not a
-- partial filter that drops anything silently.
--
-- What counts as "related" differs per layer - a clip-based layer's water-mask pieces
-- or a different layer's neighboring features, later - `role` is meant to be filtered
-- on directly by a consuming front end, so name any new one the same clear,
-- self-explanatory way rather than an abbreviation that needs a lookup.
-- One row per individual related geometry, not one row per role with everything
-- collapsed into a single multi-geometry - keeps each feature individually
-- addressable (its own label, its own style/color) for a future map UI. build_spur/
-- legacy_spur take this further and ST_Dump into one row per connected fragment (chat
-- log 2026-10-06 - "it's nice to know how many of them there are") rather than one
-- multi-geometry row per PUMA; build_sliver/legacy_sliver stay as a single combined
-- geometry per PUMA since there can be hundreds of noise fragments and the point of
-- showing them is "here's where the noise lives", not enumerating each one.
--
-- Joins back to qa__geometry_fgdb_*/qa__diffs_all on (layer, key_value) -
-- key_value here matches those tables' key_value/comparison_id for the same layer
-- and feature. Geometry in EPSG:4326/WGS84, matching geometry_diff_qa's
-- actual_geom_4326/expected_geom_4326 convention - not this product's usual
-- EPSG:2263 internal working SRID. 'build'/'legacy' are stored at full fidelity in
-- 4326; the four spur/sliver roles are a separate, deliberately LOSSY computation -
-- NOT the same path test_cases.geometry_diff_metrics uses for the real
-- symdiff_area_pct etc. metrics (those stay on EPSG:2263, unaffected by anything here).
--
-- build_spur/legacy_spur buffer the SUBTRACTED side by context_geom_noise_buffer_ft
-- (dbt_project.yml var, feet, computed on a 2263 round-trip since the buffer must be
-- in real-world units, not the 4326 degrees these geometries are otherwise stored in)
-- before differencing; build_sliver/legacy_sliver are what that buffer removes
-- (ST_Difference of the raw, unbuffered difference against the buffered one). Needed
-- because almost every symdiff fragment between build and legacy is tract-dissolve-
-- retrace needle noise that hugs the true boundary within a fraction of a foot NO
-- MATTER HOW LONG the needle is (confirmed on PUMA 3808, chat log 2026-10-06: max gap
-- 0.004 ft on a needle 2,838 ft long) - without the buffer, this table would be 99%+
-- noise, defeating its purpose of helping a human spot the real defect. A genuine
-- spur/gap (same PUMA has one - a self-touching-vertex artifact reaching 104.5 ft from
-- the true boundary, on a SMALLER 501 ft perimeter than most of the noise needles)
-- survives the buffer because its extent, not its length, is what's real.
-- Perimeter/length was tested and rejected as the filter signal for exactly this
-- reason - it doesn't separate the two populations.
--
-- label distinguishes individual rows within a role that can have more than one per
-- feature: constituent_tract's boroct id, and build_spur/legacy_spur's
-- "<key_value>_<fragment index>" (fragment 1 is the largest by area, within that
-- PUMA/role). 'build'/'legacy'/'build_sliver'/'legacy_sliver' are always exactly one
-- row per feature, so label is just key_value there, not a meaningful sub-identity.
--
-- PUMA (nypuma2010/nypuma2020) is the first populated case.
--
-- Scoped to flagged PUMAs only (qa__geometry_fgdb_nypuma2010/2020 WHERE NOT passes) -
-- confirmed 2026-10-06: unscoped, constituent_tract alone stores all 55/57 PUMAs
-- citywide (4,495 rows) when only 6/4 are ever actually flagged. This table exists to
-- help explain WHY a flagged row differs, not to mirror every PUMA regardless of
-- whether anyone needs to look at it.

WITH flagged_2010 AS (
    SELECT
        key_value,
        actual_geom_4326,
        expected_geom_4326,
        st_transform(actual_geom_4326, 2263) AS actual_geom_2263,
        st_transform(expected_geom_4326, 2263) AS expected_geom_2263
    FROM {{ ref('qa__geometry_fgdb_nypuma2010') }}
    WHERE NOT passes
),

diffs_2010 AS (
    SELECT
        key_value,
        actual_geom_4326,
        expected_geom_4326,
        st_difference(actual_geom_2263, expected_geom_2263) AS raw_build_diff,
        st_difference(expected_geom_2263, actual_geom_2263) AS raw_legacy_diff,
        st_difference(
            actual_geom_2263,
            st_buffer(expected_geom_2263, {{ var('context_geom_noise_buffer_ft', 1.0) }})
        ) AS build_spur_geom,
        st_difference(
            expected_geom_2263,
            st_buffer(actual_geom_2263, {{ var('context_geom_noise_buffer_ft', 1.0) }})
        ) AS legacy_spur_geom
    FROM flagged_2010
    -- GEOS overlay ops on an invalid input produce meaningless garbage fragments, not a
    -- real finding (confirmed: PUMA 4414/nypuma2020, inv-tv1.5 - an invalid legacy geom
    -- alone produced 147 zero-area "legacy_spur" fragments). 'build'/'legacy' roles
    -- still show the raw geometry regardless, so the invalidity itself stays visible.
    WHERE st_isvalid(actual_geom_2263) AND st_isvalid(expected_geom_2263)
),

flagged_2020 AS (
    SELECT
        key_value,
        actual_geom_4326,
        expected_geom_4326,
        st_transform(actual_geom_4326, 2263) AS actual_geom_2263,
        st_transform(expected_geom_4326, 2263) AS expected_geom_2263
    FROM {{ ref('qa__geometry_fgdb_nypuma2020') }}
    WHERE NOT passes
),

diffs_2020 AS (
    SELECT
        key_value,
        actual_geom_4326,
        expected_geom_4326,
        st_difference(actual_geom_2263, expected_geom_2263) AS raw_build_diff,
        st_difference(expected_geom_2263, actual_geom_2263) AS raw_legacy_diff,
        st_difference(
            actual_geom_2263,
            st_buffer(expected_geom_2263, {{ var('context_geom_noise_buffer_ft', 1.0) }})
        ) AS build_spur_geom,
        st_difference(
            expected_geom_2263,
            st_buffer(actual_geom_2263, {{ var('context_geom_noise_buffer_ft', 1.0) }})
        ) AS legacy_spur_geom
    FROM flagged_2020
    -- see diffs_2010's identical comment - same guard, same PUMA 4414/inv-tv1.5 reason.
    WHERE st_isvalid(actual_geom_2263) AND st_isvalid(expected_geom_2263)
),

build_spur_frags_2010 AS (
    SELECT
        key_value,
        (st_dump(build_spur_geom)).geom AS fragment
    FROM diffs_2010
),

legacy_spur_frags_2010 AS (
    SELECT
        key_value,
        (st_dump(legacy_spur_geom)).geom AS fragment
    FROM diffs_2010
),

build_spur_frags_2020 AS (
    SELECT
        key_value,
        (st_dump(build_spur_geom)).geom AS fragment
    FROM diffs_2020
),

legacy_spur_frags_2020 AS (
    SELECT
        key_value,
        (st_dump(legacy_spur_geom)).geom AS fragment
    FROM diffs_2020
)

SELECT
    'nypuma2010' AS layer,
    key_value,
    'build' AS role,
    key_value AS label,
    actual_geom_4326 AS geom
FROM flagged_2010

UNION ALL

SELECT
    'nypuma2010' AS layer,
    key_value,
    'legacy' AS role,
    key_value AS label,
    expected_geom_4326 AS geom
FROM flagged_2010

UNION ALL

SELECT
    'nypuma2010' AS layer,
    key_value,
    'build_spur' AS role,
    key_value || '_' || row_number() OVER (PARTITION BY key_value ORDER BY st_area(fragment) DESC) AS label,
    st_transform(fragment, 4326) AS geom
FROM build_spur_frags_2010

UNION ALL

SELECT
    'nypuma2010' AS layer,
    key_value,
    'legacy_spur' AS role,
    key_value || '_' || row_number() OVER (PARTITION BY key_value ORDER BY st_area(fragment) DESC) AS label,
    st_transform(fragment, 4326) AS geom
FROM legacy_spur_frags_2010

UNION ALL

SELECT
    'nypuma2010' AS layer,
    key_value,
    'build_sliver' AS role,
    key_value AS label,
    st_transform(st_difference(raw_build_diff, build_spur_geom), 4326) AS geom
FROM diffs_2010
WHERE NOT st_isempty(st_difference(raw_build_diff, build_spur_geom))

UNION ALL

SELECT
    'nypuma2010' AS layer,
    key_value,
    'legacy_sliver' AS role,
    key_value AS label,
    st_transform(st_difference(raw_legacy_diff, legacy_spur_geom), 4326) AS geom
FROM diffs_2010
WHERE NOT st_isempty(st_difference(raw_legacy_diff, legacy_spur_geom))

UNION ALL

SELECT
    'nypuma2020' AS layer,
    key_value,
    'build' AS role,
    key_value AS label,
    actual_geom_4326 AS geom
FROM flagged_2020

UNION ALL

SELECT
    'nypuma2020' AS layer,
    key_value,
    'legacy' AS role,
    key_value AS label,
    expected_geom_4326 AS geom
FROM flagged_2020

UNION ALL

SELECT
    'nypuma2020' AS layer,
    key_value,
    'build_spur' AS role,
    key_value || '_' || row_number() OVER (PARTITION BY key_value ORDER BY st_area(fragment) DESC) AS label,
    st_transform(fragment, 4326) AS geom
FROM build_spur_frags_2020

UNION ALL

SELECT
    'nypuma2020' AS layer,
    key_value,
    'legacy_spur' AS role,
    key_value || '_' || row_number() OVER (PARTITION BY key_value ORDER BY st_area(fragment) DESC) AS label,
    st_transform(fragment, 4326) AS geom
FROM legacy_spur_frags_2020

UNION ALL

SELECT
    'nypuma2020' AS layer,
    key_value,
    'build_sliver' AS role,
    key_value AS label,
    st_transform(st_difference(raw_build_diff, build_spur_geom), 4326) AS geom
FROM diffs_2020
WHERE NOT st_isempty(st_difference(raw_build_diff, build_spur_geom))

UNION ALL

SELECT
    'nypuma2020' AS layer,
    key_value,
    'legacy_sliver' AS role,
    key_value AS label,
    st_transform(st_difference(raw_legacy_diff, legacy_spur_geom), 4326) AS geom
FROM diffs_2020
WHERE NOT st_isempty(st_difference(raw_legacy_diff, legacy_spur_geom))

UNION ALL

SELECT
    'nypuma2010' AS layer,
    t.puma AS key_value,
    'constituent_tract' AS role,
    t.boroct AS label,
    st_transform(t.geom, 4326) AS geom
FROM {{ ref('stg__censustract2010') }} AS t
WHERE t.puma IN (SELECT key_value FROM flagged_2010)

UNION ALL

SELECT
    'nypuma2020' AS layer,
    t.puma AS key_value,
    'constituent_tract' AS role,
    t.boroct AS label,
    st_transform(t.geom, 4326) AS geom
FROM {{ ref('stg__censustract2020') }} AS t
WHERE t.puma IN (SELECT key_value FROM flagged_2020)
