{#
Name of the geometry column on a recipe_sources table.

Most sources are archived via ogr2ogr into postgres (column always named `wkb_geometry`), but
several are only available as parquet with a `geom`/`geometry` column from a geopandas-based
ingest path. DuckDB loads straight from each dataset's parquet archive, so it needs that
parquet column name rather than the postgres-only `wkb_geometry` convention.
#}
{% macro dcp_geom_column(source_table) -%}
    {%- set duckdb_names = {
        'dcp_mappluto_wi': 'geom',
        'dcp_pops': 'geom',
        'dcp_waterfront_access_map_wpaa': 'geom',
        'dcp_waterfront_access_map_pow': 'geom',
        'dcp_beaches': 'geometry',
        'dcp_wrp_recognized_ecological_complexes': 'geom',
        'dcp_wrp_special_natural_waterfront_areas': 'geom',
        'nysdec_freshwater_wetlands_checkzones': 'geometry',
        'nysdec_freshwater_wetlands': 'geometry',
    } -%}
    {{ duckdb_names.get(source_table, 'wkb_geometry') }}
{%- endmacro %}
