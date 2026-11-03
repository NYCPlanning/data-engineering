{#
    CSCL-LION-26: the int__lion left/right field pairs (without their left_/right_ prefix)
    that prod's LION gdb reads from the AtomicPolygons it found on the shape before a
    proto-segment's reversal: the census tracts and blocks (1990-2020) and the community
    district (ExtractorClass.cs:2390-2412 and 3934-4338).
#}
{% macro lion_gdb_census_side_fields() -%}
    {{ return([
        'community_district',
        '1990_census_tract_basic', '1990_census_tract_suffix',
        '2000_census_tract_basic', '2000_census_tract_suffix',
        '2010_census_tract_basic', '2010_census_tract_suffix',
        '2020_census_tract_basic', '2020_census_tract_suffix',
        '2000_census_block_basic', '2000_census_block_suffix',
        '2010_census_block_basic', '2010_census_block_suffix',
        '2020_census_block_basic', '2020_census_block_suffix',
    ]) }}
{%- endmacro %}

{#
    A REPLACE list swapping those fields' left and right values where `swapped` is true:
        SELECT lion.* REPLACE ({{ swap_lion_gdb_census_sides('lion', 'protos.swapped') }})
#}
{% macro swap_lion_gdb_census_sides(relation, swapped) -%}
    {%- for field in lion_gdb_census_side_fields() %}
    CASE WHEN {{ swapped }} THEN {{ relation }}.right_{{ field }} ELSE {{ relation }}.left_{{ field }} END
        AS left_{{ field }},
    CASE WHEN {{ swapped }} THEN {{ relation }}.left_{{ field }} ELSE {{ relation }}.right_{{ field }} END
        AS right_{{ field }}{{ ',' if not loop.last }}
    {%- endfor %}
{%- endmacro %}

{# True where any of those fields differs between the two sides. #}
{% macro lion_gdb_census_sides_differ(relation) -%}
    (
    {%- for field in lion_gdb_census_side_fields() %}
        {{ relation }}.left_{{ field }} IS DISTINCT FROM {{ relation }}.right_{{ field }}{{ ' OR' if not loop.last }}
    {%- endfor %}
    )
{%- endmacro %}
