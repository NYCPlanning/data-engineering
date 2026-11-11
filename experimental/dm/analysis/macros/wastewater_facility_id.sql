{#-
DEP's wastewater facility names to wastewater_facility_id (Wards Island or WARDS ISLAND WWTP to wards_island), the
same rule scripts/extract_dep_wastewater_map.py uses. The MS4 map shortens 26th Ward to 26 WARD.
-#}
{% macro wastewater_facility_id(column) -%}
replace(
    regexp_replace(trim(regexp_replace(lower({{ column }}), '\s+wwtp$', '')), '\W+', '_', 'g'),
    '26_ward',
    '26th_ward'
)
{%- endmacro %}
