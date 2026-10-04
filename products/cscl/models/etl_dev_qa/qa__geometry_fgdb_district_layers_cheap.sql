{{ config(materialized='table', tags=['qa']) }}

-- Combined geometry_diff_qa_cheap (macros/geometry_diff_qa.sql) run across the district
-- gdb layers too large for the full check's per-row ST_SymDifference/ST_HausdorffDistance
-- cost (~0.5-1.2s/row empirically -> 5-20+ hours each at these row counts): nyap
-- (69,786 rows), nycb2010/nycb2010wi (38,797/39,141), nycb2020/nycb2020wi
-- (37,588/37,984) - chat log 2026-10-05. See qa__geometry_fgdb_district_layers.sql for
-- every other district_gdb layer (full check) and geometry_diff_qa_cheap's own
-- docstring for exactly what this cheaper check does and doesn't catch.
-- nycb2010 is not listed: it has its own dedicated model (qa__diffs_fgdb_nycb2010), and
-- listing it here too double-reports every differing block in qa__diffs_all.
{% set layers = [
    {'name': 'nyap', 'key': '"ATOMICID"'},
    {'name': 'nycb2010wi', 'key': '"BCTCB2010"'},
    {'name': 'nycb2020', 'key': '"BCTCB2020"'},
    {'name': 'nycb2020wi', 'key': '"BCTCB2020"'},
] %}

{% for l in layers %}
    SELECT
        '{{ l.name }}' AS source_layer,
        *
    FROM (
    {{ geometry_diff_qa_cheap(
        build_relation=ref('gdb_' ~ l.name),
        prod_relation=adapter.get_relation(database="db-cscl", schema="production_outputs", identifier="fgdb_" ~ l.name),
        key_column=l.key,
        prod_key_column=l.get('prod_key')
    ) }}
    ) AS t
    {{ "UNION ALL" if not loop.last else "" }}
{% endfor %}
