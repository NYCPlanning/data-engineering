{% macro geometry_diff_qa(build_relation, prod_relation, key_column, build_geom_column='geom', prod_geom_column='shape', prod_key_column=none) %}
{#-
  Full-layer geometry QA: joins every build row to its prod counterpart by key, runs
  geometry_diff_metrics (macros/geometry_diff_qa.sql) on each pair, and
  flags pass/fail against the same thresholds qa__test_cases_geometry uses. See that
  model and the function's own docstring for why area/perimeter diffing alone isn't
  enough.

  key_column: build-side key, as it appears in the build relation (quote if it's a
    case-sensitive gdb-style column, e.g. '"FireBN"'). Single-column only:
    a multi-column declared key (lion_outputs.csv's "FireCoType|FireCoNum" style)
    needs a concatenation expression passed as key_column instead.
  prod_key_column: defaults to key_column.lower() with quoting stripped, matching how
    the prod snapshot lowercases every prod column - override if that guess is wrong.
  build_geom_column/prod_geom_column: default to how gdb_* models and the prod snapshot's
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
  reprojection noise to sub-foot metrics. actual_geom/expected_geom (no _4326 suffix,
  native SRID, unlinearized) feed the metrics.
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
            WHEN count(*) = 1 THEN any_value({{ build_geom_column }})
            ELSE st_union_agg(try(st_makevalid(linearize({{ build_geom_column }}))))
        END AS actual_geom
    FROM {{ build_relation }}
    GROUP BY 1
),
prod AS (
    SELECT
        {{ prod_key_column }}::text AS key_value,
        CASE
            WHEN count(*) = 1 THEN any_value({{ prod_geom_column }})
            ELSE st_union_agg(try(st_makevalid(linearize({{ prod_geom_column }}))))
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
        st_transform(linearize(j.actual_geom), 'EPSG:2263', 'EPSG:4326', always_xy := true) AS actual_geom_4326,
        st_transform(linearize(j.expected_geom), 'EPSG:2263', 'EPSG:4326', always_xy := true) AS expected_geom_4326,
        j.actual_area_sqft,
        j.expected_area_sqft,
        j.symdiff_area_pct,
        j.hausdorff_ft,
        j.symdiff_boundary_len_ft,
        j.symdiff_fragment_count,
        j.max_ring_eccentricity,
        j.perimeter_pct_diff
    FROM (
        {{ geometry_diff_metrics('joined', 'actual_geom', 'expected_geom') }}
    ) AS j
),
flagged AS (
    SELECT
        *,
        list_filter([
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
        ], x -> x IS NOT NULL) AS failing_checks
    FROM metrics
)

SELECT
    *,
    (len(failing_checks) = 0) AS passes
FROM flagged
{% endmacro %}


{% macro geometry_diff_qa_cheap(build_relation, prod_relation, key_column, build_geom_column='geom', prod_geom_column='shape', prod_key_column=none) %}
{#-
  Cheap variant of geometry_diff_qa for layers too large to run full-table
  (test_cases.geometry_diff_metrics' ST_SymDifference + simplified ST_HausdorffDistance
  cost ~0.5-1.2s/row empirically - fine at dozens to low thousands of rows, not at
  nyap's 69,786 or nycb2010/2010wi/2020/2020wi's ~38-39k each, which would add 5-20+
  hours per layer per build).

  Computes only perimeter_pct_diff and area_pct_diff - a direct ST_Perimeter/ST_Area
  comparison on each side independently, no overlay operation at all (no
  ST_SymDifference, no ST_HausdorffDistance) - cheap enough to run on every row of even
  the largest layers. Catches the exact class of bug that motivated adding
  perimeter_pct_diff to the full framework in the first place (nearest-N rounding-
  boundary false positives in the older qa_int__prod_fgdb_*/​*_by_field comparisons),
  just without symdiff_area_pct/hausdorff_ft/symdiff_fragment_
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
            WHEN count(*) = 1 THEN any_value(linearize({{ build_geom_column }}))
            ELSE st_union_agg(try(st_makevalid(linearize({{ build_geom_column }}))))
        END AS actual_geom
    FROM {{ build_relation }}
    GROUP BY 1
),
prod AS (
    SELECT
        {{ prod_key_column }}::text AS key_value,
        CASE
            WHEN count(*) = 1 THEN any_value(linearize({{ prod_geom_column }}))
            ELSE st_union_agg(try(st_makevalid(linearize({{ prod_geom_column }}))))
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
        list_filter([
            CASE WHEN only_in_build THEN 'only_in_build' END,
            CASE WHEN only_in_prod THEN 'only_in_prod' END,
            CASE WHEN abs(perimeter_pct_diff) > {{ var('geom_check_perimeter_pct', 1) }}
                THEN 'perimeter_pct_diff' END,
            CASE WHEN abs(area_pct_diff) > {{ var('geom_check_area_pct_diff', 1) }}
                THEN 'area_pct_diff' END
        ], x -> x IS NOT NULL) AS failing_checks
    FROM metrics
)

SELECT
    *,
    (len(failing_checks) = 0) AS passes
FROM flagged
{% endmacro %}


{% macro geometry_diff_metrics(input_relation, actual_col, expected_col, min_fragment_sqft=100) %}
{%- set hd_tol = var('geom_check_hausdorff_tolerance_ft', 1.0) -%}
{#-
  Returns every column of input_relation plus the eight metrics (actual_area_sqft,
  expected_area_sqft, symdiff_area_pct, hausdorff_ft, symdiff_boundary_len_ft,
  symdiff_fragment_count, max_ring_eccentricity, perimeter_pct_diff). Inputs must already
  be in the working SRID (EPSG:2263, no transform here). It is a macro that emits SQL,
  not a table function (DuckDB macros can't hold the subqueries it needs).
  Fragments are kept as a list per row instead of a correlated subquery.
  hausdorff_ft is not the exact discrete Hausdorff distance: DuckDB has none, and a
  per-vertex st_distance over every vertex runs out of memory on the district layers.
  Instead only boundary pieces further than hd_tol (var geom_check_hausdorff_tolerance_ft,
  1 ft) from the other geometry are measured, vertex by vertex. So the result is exact
  above hd_tol and 0 below it, against a 50 ft threshold.
-#}
SELECT
    f.* EXCLUDE (_a, _e, _a_s, _e_s, _frags, _rings),
    st_area(_a) AS actual_area_sqft,
    st_area(_e) AS expected_area_sqft,
    CASE WHEN _e IS NOT NULL THEN
        coalesce(list_sum(list_transform(_frags, g -> st_area(g))), 0)
        / nullif(st_area(_a), 0) * 100
    END AS symdiff_area_pct,
    CASE WHEN _e IS NOT NULL THEN
        greatest(
            coalesce(list_max(list_transform(
                st_dump(st_points(st_difference(st_boundary(_a_s), st_buffer(_e_s, {{ hd_tol }})))),
                p -> st_distance(p.geom, _e_s)
            )), 0.0),
            coalesce(list_max(list_transform(
                st_dump(st_points(st_difference(st_boundary(_e_s), st_buffer(_a_s, {{ hd_tol }})))),
                p -> st_distance(p.geom, _a_s)
            )), 0.0)
        )
    END AS hausdorff_ft,
    CASE WHEN _e IS NOT NULL THEN
        coalesce(list_sum(list_transform(_frags, g -> st_perimeter(g))), 0)
    END AS symdiff_boundary_len_ft,
    CASE WHEN _e IS NOT NULL THEN len(_frags)::INTEGER END AS symdiff_fragment_count,
    list_max(list_transform(
        _rings,
        r -> st_length(r) / greatest(sqrt(abs(st_area(st_makepolygon(r)))), 0.0001)
    )) AS max_ring_eccentricity,
    CASE WHEN _e IS NOT NULL THEN
        (st_perimeter(_a) - st_perimeter(_e)) / nullif(st_perimeter(_e), 0) * 100
    END AS perimeter_pct_diff
FROM (
    SELECT
        *,
        CASE WHEN _a IS NOT NULL AND _e IS NOT NULL THEN
            list_filter(
                list_transform(st_dump(st_symdifference(_a, _e)), d -> d.geom),
                g -> st_area(g) >= {{ min_fragment_sqft }}
            )
        END AS _frags,
        flatten(list_transform(
            list_filter(list_transform(st_dump(_a), d -> d.geom), p -> st_geometrytype(p) = 'POLYGON'),
            p -> list_prepend(
                st_exteriorring(p),
                list_transform(range(1, st_numinteriorrings(p) + 1), i -> st_interiorringn(p, i::INTEGER))
            )
        )) AS _rings
    FROM (
        SELECT
            *,
            linearize({{ actual_col }}) AS _a,
            linearize({{ expected_col }}) AS _e,
            st_simplify(linearize({{ actual_col }}), 0.05) AS _a_s,
            st_simplify(linearize({{ expected_col }}), 0.05) AS _e_s
        FROM {{ input_relation }}
    )
) AS f
{% endmacro %}


{% macro prod_fgdb_linear(identifier, geom_column='shape') %}
{#-
  production_outputs.<identifier>, loaded from the production_outputs parquet bundle
  (poc_validation/build_prod_snapshot.py). Its geometry is already linear: prod's curves
  were linearized by dcpy.geospatial.curves when the bundle was built, exactly as the
  build's own sources are. Prod geometry isn't always valid (e.g. 6 of fgdb_nyhez's 8
  features are invalid once linearized, where rings nearly touch), and the overlay metrics need valid input: st_makevalid repairs it here, at
  comparison time. The snapshot itself keeps prod as it is.
-#}
(
    SELECT * REPLACE (st_makevalid("{{ geom_column }}") AS "{{ geom_column }}")
    FROM {{ adapter.get_relation(database=target.database, schema="production_outputs", identifier=identifier) }}
)
{% endmacro %}


{% macro geometry_diff_qa_district_layers() %}
{#-
  The district_gdb layers qa__geometry_fgdb_district_layers checks: every layer in
  district_review_layers() except those checked elsewhere. nyap and the census block
  layers are too large for the full check (qa__geometry_fgdb_district_layers_cheap,
  qa__diffs_fgdb_nycb2010); nycd, nyfb, nypuma2010 and nypuma2020 have their own models.
  nyura is left out: prod's fgdb_nyura is empty and carries nybid's schema, so there is
  nothing to compare. Narrowed by var district_geometry_qa_only (a list of names).
-#}
{%- set elsewhere = [
    'nyap', 'nycb2010', 'nycb2010wi', 'nycb2020', 'nycb2020wi',
    'nycd', 'nyfb', 'nypuma2010', 'nypuma2020', 'nyura',
] -%}
{%- set only = var('district_geometry_qa_only', []) -%}
{%- set layers = district_review_layers() | rejectattr('name', 'in', elsewhere) | list -%}
{%- if only %}{% set layers = layers | selectattr('name', 'in', only) | list %}{% endif -%}
{%- do return(layers) -%}
{% endmacro %}


{% macro geometry_diff_qa_district_layer(layer) %}
{#-
  One layer's rows of qa__geometry_fgdb_district_layers: geometry_diff_qa on it, plus
  passes_without_prod_hairlines, the same check against prod with its hairlines removed
  (prod_fgdb_linear_without_hairlines), run on the failing keys only. A row that fails
  but passes without them differs from prod only by prod's hairline slivers
  (CSCL-DISTRICTS-01).
-#}
{%- set prod_key = layer.get('prod_key') or (layer.key | replace('"', '') | lower) -%}
-- Wrapped in a subquery so the post-hooks can INSERT it.
SELECT * FROM (
WITH t AS (
    {{ geometry_diff_qa(
        build_relation=ref('gdb_' ~ layer.name),
        prod_relation=prod_fgdb_linear('fgdb_' ~ layer.name),
        key_column=layer.key,
        prod_key_column=layer.get('prod_key')
    ) }}
),

failing AS (
    SELECT key_value FROM t WHERE NOT passes
),

h AS (
    {{ geometry_diff_qa(
        build_relation='(SELECT * FROM ' ~ ref('gdb_' ~ layer.name) ~ ' WHERE ('
            ~ layer.key ~ ')::text IN (SELECT key_value FROM failing))',
        prod_relation=prod_fgdb_linear_without_hairlines(
            'fgdb_' ~ layer.name,
            where='(' ~ prod_key ~ ')::text IN (SELECT key_value FROM failing)'
        ),
        key_column=layer.key,
        prod_key_column=layer.get('prod_key')
    ) }}
)

SELECT
    '{{ layer.name }}' AS source_layer,
    t.*,
    coalesce(h.passes, t.passes) AS passes_without_prod_hairlines
FROM t
LEFT JOIN h ON t.key_value = h.key_value
) AS layer_rows
{% endmacro %}


{% macro prod_fgdb_linear_without_hairlines(identifier, geom_column='shape', where='TRUE') %}
{#-
  prod_fgdb_linear without prod's hairlines: anything narrower than var
  district_hairline_width_ft, removed by a morphological opening (shrink by half the
  width, grow back). Prod's district polygons carry hundreds of these along their
  outlines, about 0.0001 ft wide, as separate parts and as spikes off a ring; they add
  parts and length but no area (CSCL-DISTRICTS-01). The opening moves the rest of the
  boundary by far less than the width. where: a filter on the prod rows, since the
  opening is slow on the large layers.
-#}
{%- set half_width = var('district_hairline_width_ft') / 2 -%}
(
    SELECT * REPLACE (
        st_buffer(st_buffer(st_makevalid("{{ geom_column }}"), -{{ half_width }}), {{ half_width }})
            AS "{{ geom_column }}"
    )
    FROM {{ adapter.get_relation(database=target.database, schema="production_outputs", identifier=identifier) }}
    WHERE {{ where }}
)
{% endmacro %}
