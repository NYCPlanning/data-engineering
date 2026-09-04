{#
Reproject a geometry to a target EPSG SRID.

DuckDB's spatial extension needs both source and target CRS -- the source is read from the
geometry's own embedded CRS metadata via the 2-arg form, target given as an 'EPSG:<n>' string.
It defaults to strict per-CRS axis order (e.g. lat,lon for EPSG:4326) rather than the
GIS-conventional x,y order our WKT is stored in, hence always_xy := true.
#}
{% macro dcp_st_transform(geom, srid) -%}
    ST_TRANSFORM({{ geom }}, 'EPSG:{{ srid }}', always_xy := true)
{%- endmacro %}
