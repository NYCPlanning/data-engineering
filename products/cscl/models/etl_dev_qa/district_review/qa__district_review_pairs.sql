{{ config(
    materialized='table',
    tags=['qa'],
    post_hook=district_review_layer_hooks('district_review_pair')
) }}

-- Every district of every district_gdb layer, ours next to prod's (production_outputs,
-- the previous release), one row per (layer, district key). See
-- macros/district_review.sql. Feeds the district review FGDB
-- (poc_validation/district_review_gdb.py) through the other qa__district_review_*
-- models. var district_review_only (a list of layer names) narrows it for iterating.
--
-- Built one layer per statement: this body is the first layer, post-hooks insert the
-- rest (district_review_layer_hooks says why).
{%- for layer in district_review_selected_layers() %}
-- depends_on: {{ ref('gdb_' ~ layer.name) }}
{%- endfor %}

{{ district_review_pair(district_review_selected_layers()[0]) }}
