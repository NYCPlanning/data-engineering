{{ config(materialized='table', tags=['qa']) }}

-- Every district of every district_gdb layer, ours next to prod's (production_outputs,
-- the previous release), one row per (layer, district key). See
-- macros/district_review.sql. Feeds the district review FGDB
-- (poc_validation/district_review_gdb.py) through the other qa__district_review_*
-- models. var district_review_only (a list of layer names) narrows it for iterating.

{%- set only = var('district_review_only', []) %}
{%- set layers = district_review_layers() %}
{%- if only %}{% set layers = layers | selectattr('name', 'in', only) | list %}{% endif %}

{% for layer in layers %}
    SELECT * FROM ({{ district_review_pair(layer) }}) AS {{ layer.name }}
    {{ "UNION ALL" if not loop.last else "" }}
{% endfor %}
