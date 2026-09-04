{#
Relabel a geometry's SRID without reprojecting coordinates -- for sources whose coordinates
are already in the target plane but are mislabeled at ingest (see call sites for why).
#}
{% macro dcp_st_setsrid(geom, srid) -%}
    ST_SETCRS({{ geom }}, 'EPSG:{{ srid }}')
{%- endmacro %}
