{{ config(materialized='table', tags=['qa']) }}

-- Combined full geometry_diff_qa (macros/geometry_diff_qa.sql) run across every
-- district_gdb layer small enough to run full-table (the per-row cost - ST_SymDifference
-- plus a simplified ST_HausdorffDistance - runs ~0.5-1.2s/row empirically, fine up to a
-- few thousand rows, not at nyap/nycb2010/nycb2010wi/nycb2020/nycb2020wi's 38k-70k rows,
-- which get the cheaper geometry_diff_qa_cheap instead - see
-- qa__geometry_fgdb_district_layers_cheap.sql). nycd/nyfb/nypuma2010/nypuma2020 already
-- had their own bespoke models before this one existed (chat log 2026-10-04/05) and are
-- left as-is rather than folded in here, to avoid touching working models without need.
--
-- One combined table with a source_layer column, not 34 separate model files - same
-- underlying per-layer SQL (geometry_diff_qa expands to one full query per layer,
-- UNION ALL'd together), just far less file/boilerplate churn for a set this large
-- (chat log 2026-10-05).
--
-- key_column follows each layer's declared key in seeds/lion_outputs.csv. A composite
-- (pipe-separated) declared key gets an explicit concat expression for both key_column
-- and prod_key_column - geometry_diff_qa's default prod_key_column inference (lower,
-- strip quotes) only works for a single bare column name.
--
-- nyura deliberately excluded from the catalog below: prod's fgdb_nyura carries a
-- stale copy of nybid's schema (bidid/bid/borough, no uraid column at all) - a known
-- structural diff, not a fixable key mismatch (compare_gdb.py's KNOWN_STRUCTURAL_DIFFS,
-- Bug 013/inv-9k8). Prod's table is also empty (0 rows) - nothing meaningful to compare.
--
-- DORMANT BY DEFAULT (chat log 2026-10-05): running the full catalog below took over
-- 10 minutes (33 layers, several thousand rows each at the slower mid-size layers -
-- nyct2010/wi, nyct2020/wi, nyed/wi), too slow for a routine build. `enabled` controls
-- which layers actually run - add a name to it to dive into that layer's geometry
-- shape QA, remove it when done. The full catalog stays here as reference so you don't
-- have to re-derive key columns/types from scratch each time.
{% set enabled = ['nycdta2020', 'nynta2010', 'nynta2020', 'nysd'] %}

{% set all_layers = [
    {'name': 'nyad', 'key': '"AssemDist"'},
    {'name': 'nyadwi', 'key': '"AssemDist"'},
    {'name': 'nybb', 'key': '"BoroCode"'},
    {'name': 'nybbwi', 'key': '"BoroCode"'},
    {'name': 'nybid', 'key': '"BIDID"'},
    {'name': 'nycc', 'key': '"CounDist"'},
    {'name': 'nyccwi', 'key': '"CounDist"'},
    {'name': 'nycdta2020', 'key': '"CDTA2020"'},
    {'name': 'nycdwi', 'key': '"BoroCD"'},
    {'name': 'nycg', 'key': '"CongDist"'},
    {'name': 'nycgwi', 'key': '"CongDist"'},
    {'name': 'nyct2010', 'key': '"BoroCT2010"'},
    {'name': 'nyct2010wi', 'key': '"BoroCT2010"'},
    {'name': 'nyct2020', 'key': '"BoroCT2020"'},
    {'name': 'nyct2020wi', 'key': '"BoroCT2020"'},
    {'name': 'nyed', 'key': '"ElectDist"'},
    {'name': 'nyedwi', 'key': '"ElectDist"'},
    {'name': 'nyfc', 'key': '("FireCoType"::text || \'|\' || "FireCoNum"::text)',
     'prod_key': "(firecotype::text || '|' || fireconum::text)"},
    {'name': 'nyfd', 'key': '"FireDiv"'},
    {'name': 'nyha', 'key': '("BoroCode"::text || \'|\' || "HealthArea"::text)',
     'prod_key': "(borocode::text || '|' || healtharea::text)"},
    {'name': 'nyhc', 'key': '"HCentDist"'},
    {'name': 'nyhd', 'key': '"NAME"'},
    {'name': 'nyhez', 'key': '"HURRICANE_EVACUATION_ZONE"'},
    {'name': 'nymc', 'key': '("BoroCode"::text || \'|\' || "MuniCourt"::text)',
     'prod_key': "(borocode::text || '|' || municourt::text)"},
    {'name': 'nymcea', 'key': '("BOROCODE"::text || \'|\' || "MCEA"::text)',
     'prod_key': "(borocode::text || '|' || mcea::text)"},
    {'name': 'nymcwi', 'key': '("BoroCode"::text || \'|\' || "MuniCourt"::text)',
     'prod_key': "(borocode::text || '|' || municourt::text)"},
    {'name': 'nynta2010', 'key': '"NTACode"'},
    {'name': 'nynta2020', 'key': '"NTA2020"'},
    {'name': 'nypp', 'key': '"Precinct"'},
    {'name': 'nysd', 'key': '"SchoolDist"'},
    {'name': 'nyss', 'key': '"StSenDist"'},
    {'name': 'nysswi', 'key': '"StSenDist"'},
    {'name': 'nyzip', 'key': '"ZIP_CODE"'},
] %}

{% set layers = all_layers | selectattr('name', 'in', enabled) | list %}

{% if layers | length > 0 %}
    {% for l in layers %}
        SELECT
            '{{ l.name }}' AS source_layer,
            *
        FROM (
    {{ geometry_diff_qa(
        build_relation=ref('gdb_' ~ l.name),
        prod_relation=adapter.get_relation(database="db-cscl", schema="production_outputs", identifier="fgdb_" ~ l.name),
        key_column=l.key,
        prod_key_column=l.get('prod_key')
    ) }}
        ) AS t
        {{ "UNION ALL" if not loop.last else "" }}
    {% endfor %}
{% else %}
-- Nothing enabled - empty but correctly-shaped result so downstream models/tests
-- still build cleanly. Add a layer name to `enabled` above to activate it.
SELECT
    NULL::text AS source_layer,
    NULL::text AS key_value,
    NULL::boolean AS only_in_prod,
    NULL::boolean AS only_in_build,
    NULL::geometry AS actual_geom_4326,
    NULL::geometry AS expected_geom_4326,
    NULL::double precision AS actual_area_sqft,
    NULL::double precision AS expected_area_sqft,
    NULL::double precision AS symdiff_area_pct,
    NULL::double precision AS hausdorff_ft,
    NULL::double precision AS symdiff_boundary_len_ft,
    NULL::integer AS symdiff_fragment_count,
    NULL::double precision AS max_ring_eccentricity,
    NULL::double precision AS perimeter_pct_diff,
    NULL::text[] AS failing_checks,
    NULL::boolean AS passes
WHERE FALSE
{% endif %}
