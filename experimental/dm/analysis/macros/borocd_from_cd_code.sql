{#- Community district codes like BK10 or BK 10 (borough letters and district number) to borocd (310). -#}
{% macro borocd_from_cd_code(column) -%}
CASE left({{ column }}, 2)
    WHEN 'MN' THEN 1
    WHEN 'BX' THEN 2
    WHEN 'BK' THEN 3
    WHEN 'QN' THEN 4
    WHEN 'SI' THEN 5
END * 100 + trim(substr({{ column }}, 3))::integer
{%- endmacro %}
