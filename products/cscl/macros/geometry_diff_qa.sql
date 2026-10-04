{% macro geometry_diff_qa(build_relation, prod_relation, key_column, build_geom_column='geom', prod_geom_column='shape', prod_key_column=none) %}
{#-
  Full-layer geometry QA: joins every build row to its prod counterpart by key, runs
  test_cases.geometry_diff_metrics (macros/create_pg_functions.sql) on each pair, and
  flags pass/fail against the same thresholds qa__test_cases_geometry uses. See that
  model and the function's own docstring for why area/perimeter diffing alone isn't
  enough (products/cscl chat log, 2026-10-04/05).

  key_column: build-side key, as it appears in the build relation (quote if it's a
    case-sensitive gdb-style column, e.g. '"FireBN"'). Single-column only for now -
    a multi-column declared key (lion_outputs.csv's "FireCoType|FireCoNum" style)
    needs a concatenation expression passed as key_column instead.
  prod_key_column: defaults to key_column.lower() with quoting stripped, matching how
    ogr2ogr lowercases every prod column on load - override if that guess is wrong.
  build_geom_column/prod_geom_column: default to how gdb_* models and ogr2ogr-loaded
    fgdb_* tables name their geometry column respectively - override per layer if
    different.

  Rows only on one side (only_in_build/only_in_prod) get no metrics at all - there's
  no pair to compare, and that's its own, more basic QA concern (row-count/key
  coverage), not a geometry-shape one.

  actual_geom_4326/expected_geom_4326 carry through to the final output for eventual
  display in a Python mapping tool (folium, geopandas .explore(), etc. - all expect
  lat/lon) - linearized (prod's FGDB load can be curved MultiSurface, which most
  geometry tooling, shapely/geopandas included, rejects outright) and transformed to
  EPSG:4326/WGS84, separately from the geometry the metrics below are computed from.
  Deliberately NOT the same transform path test_cases.geometry_diff_metrics uses
  internally (linearize + transform to this product's usual EPSG:2263 working SRID) -
  reprojecting display geometry to 4326 and reprojecting measurement geometry to 2263
  are two different concerns, and round-tripping the same geometry through both
  (native SRID -> 4326 -> back to 2263 inside the metrics function) would add its own
  tiny reprojection noise on top of everything this investigation has already spent on
  chasing sub-foot precision - chat log 2026-10-06. actual_geom/expected_geom (no
  _4326 suffix, native SRID, unlinearized) still feed the metrics exactly as before.
-#}
    {%- set prod_key_column = prod_key_column or (key_column | replace('"', '') | lower) -%}

-- One row per key on each side: a layer that stores a district as several features
-- (e.g. nysd's SD 10, once per borough) is dissolved first, so the key join compares
-- whole districts rather than every piece against every other piece. Single-row keys
-- pass through untouched.
WITH build AS (
    SELECT
        {{ key_column }}::text AS key_value,
        CASE
            WHEN count(*) = 1 THEN (array_agg({{ build_geom_column }}))[1]
            ELSE st_union(st_makevalid(linearize({{ build_geom_column }})))
        END AS actual_geom
    FROM {{ build_relation }}
    GROUP BY 1
),
prod AS (
    SELECT
        {{ prod_key_column }}::text AS key_value,
        CASE
            WHEN count(*) = 1 THEN (array_agg({{ prod_geom_column }}))[1]
            ELSE st_union(st_makevalid(linearize({{ prod_geom_column }})))
        END AS expected_geom
    FROM {{ prod_relation }}
    GROUP BY 1
),
joined AS (
    SELECT
        coalesce(b.key_value, p.key_value) AS key_value,
        b.actual_geom,
        p.expected_geom,
        (b.key_value IS NULL) AS only_in_prod,
        (p.key_value IS NULL) AS only_in_build
    FROM build AS b
    FULL OUTER JOIN prod AS p ON b.key_value = p.key_value
),
metrics AS (
    SELECT
        j.key_value,
        j.only_in_prod,
        j.only_in_build,
        st_transform(linearize(j.actual_geom), 4326) AS actual_geom_4326,
        st_transform(linearize(j.expected_geom), 4326) AS expected_geom_4326,
        m.*
    FROM joined AS j
    LEFT JOIN LATERAL test_cases.geometry_diff_metrics(j.actual_geom, j.expected_geom) AS m
        ON j.actual_geom IS NOT NULL
),
flagged AS (
    SELECT
        *,
        array_remove(ARRAY[
            CASE WHEN only_in_build THEN 'only_in_build' END,
            CASE WHEN only_in_prod THEN 'only_in_prod' END,
            CASE WHEN symdiff_area_pct > {{ var('geom_check_symdiff_area_pct', 1) }}
                THEN 'symdiff_area_pct' END,
            CASE WHEN hausdorff_ft > {{ var('geom_check_hausdorff_ft', 50) }}
                THEN 'hausdorff_ft' END,
            CASE WHEN symdiff_boundary_len_ft > {{ var('geom_check_symdiff_boundary_len_ft', 100) }}
                THEN 'symdiff_boundary_len_ft' END,
            CASE WHEN max_ring_eccentricity > {{ var('geom_check_max_ring_eccentricity', 50) }}
                THEN 'max_ring_eccentricity' END,
            CASE WHEN abs(perimeter_pct_diff) > {{ var('geom_check_perimeter_pct', 1) }}
                THEN 'perimeter_pct_diff' END
        ], NULL) AS failing_checks
    FROM metrics
)

SELECT
    *,
    (array_length(failing_checks, 1) IS NULL) AS passes
FROM flagged
{% endmacro %}


{% macro geometry_diff_qa_cheap(build_relation, prod_relation, key_column, build_geom_column='geom', prod_geom_column='shape', prod_key_column=none) %}
{#-
  Cheap variant of geometry_diff_qa for layers too large to run full-table
  (test_cases.geometry_diff_metrics' ST_SymDifference + simplified ST_HausdorffDistance
  cost ~0.5-1.2s/row empirically - fine at dozens to low thousands of rows, not at
  nyap's 69,786 or nycb2010/2010wi/2020/2020wi's ~38-39k each, which would add 5-20+
  hours per layer per build - chat log 2026-10-05).

  Computes only perimeter_pct_diff and area_pct_diff - a direct ST_Perimeter/ST_Area
  comparison on each side independently, no overlay operation at all (no
  ST_SymDifference, no ST_HausdorffDistance) - cheap enough to run on every row of even
  the largest layers. Catches the exact class of bug that motivated adding
  perimeter_pct_diff to the full framework in the first place (nearest-N rounding-
  boundary false positives in the older qa_int__prod_fgdb_*/​*_by_field comparisons,
  chat log 2026-10-05), just without symdiff_area_pct/hausdorff_ft/symdiff_fragment_
  count/max_ring_eccentricity's deeper (but expensive) shape-defect detection.

  Same key_column/prod_key_column/build_geom_column/prod_geom_column semantics as
  geometry_diff_qa - see that macro's docstring.
-#}
    {%- set prod_key_column = prod_key_column or (key_column | replace('"', '') | lower) -%}

-- One row per key on each side (multi-feature districts dissolved) - see
-- geometry_diff_qa.
WITH build AS (
    SELECT
        {{ key_column }}::text AS key_value,
        CASE
            WHEN count(*) = 1 THEN (array_agg(st_transform(linearize({{ build_geom_column }}), 2263)))[1]
            ELSE st_union(st_makevalid(st_transform(linearize({{ build_geom_column }}), 2263)))
        END AS actual_geom
    FROM {{ build_relation }}
    GROUP BY 1
),
prod AS (
    SELECT
        {{ prod_key_column }}::text AS key_value,
        CASE
            WHEN count(*) = 1 THEN (array_agg(st_transform(linearize({{ prod_geom_column }}), 2263)))[1]
            ELSE st_union(st_makevalid(st_transform(linearize({{ prod_geom_column }}), 2263)))
        END AS expected_geom
    FROM {{ prod_relation }}
    GROUP BY 1
),
joined AS (
    SELECT
        coalesce(b.key_value, p.key_value) AS key_value,
        b.actual_geom,
        p.expected_geom,
        (b.key_value IS NULL) AS only_in_prod,
        (p.key_value IS NULL) AS only_in_build
    FROM build AS b
    FULL OUTER JOIN prod AS p ON b.key_value = p.key_value
),
metrics AS (
    SELECT
        key_value,
        only_in_prod,
        only_in_build,
        st_area(actual_geom) AS actual_area_sqft,
        st_area(expected_geom) AS expected_area_sqft,
        CASE WHEN expected_geom IS NOT NULL THEN
            (st_perimeter(actual_geom) - st_perimeter(expected_geom))
            / nullif(st_perimeter(expected_geom), 0) * 100
        END AS perimeter_pct_diff,
        CASE WHEN expected_geom IS NOT NULL THEN
            (st_area(actual_geom) - st_area(expected_geom))
            / nullif(st_area(expected_geom), 0) * 100
        END AS area_pct_diff
    FROM joined
),
flagged AS (
    SELECT
        *,
        array_remove(ARRAY[
            CASE WHEN only_in_build THEN 'only_in_build' END,
            CASE WHEN only_in_prod THEN 'only_in_prod' END,
            CASE WHEN abs(perimeter_pct_diff) > {{ var('geom_check_perimeter_pct', 1) }}
                THEN 'perimeter_pct_diff' END,
            CASE WHEN abs(area_pct_diff) > {{ var('geom_check_area_pct_diff', 1) }}
                THEN 'area_pct_diff' END
        ], NULL) AS failing_checks
    FROM metrics
)

SELECT
    *,
    (array_length(failing_checks, 1) IS NULL) AS passes
FROM flagged
{% endmacro %}
