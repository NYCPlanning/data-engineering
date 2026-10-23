{{ config(
    materialized='table',
    tags=['qa'],
    post_hook=district_review_layer_hooks(
        'geometry_diff_qa_district_layer', 'geometry_diff_qa_district_layers'
    )
) }}

-- Full geometry_diff_qa (macros/geometry_diff_qa.sql) on every district_gdb layer that
-- isn't checked by another model: geometry_diff_qa_district_layers lists them and says
-- which are left out and why. One row per (source_layer, key). var
-- district_geometry_qa_only (a list of layer names) narrows it for iterating.
--
-- Built one layer per statement, as the district review is: this body is the first
-- layer, post-hooks insert the rest (district_review_layer_hooks says why).
{%- for layer in geometry_diff_qa_district_layers() %}
-- depends_on: {{ ref('gdb_' ~ layer.name) }}
{%- endfor %}

{{ geometry_diff_qa_district_layer(geometry_diff_qa_district_layers()[0]) }}
