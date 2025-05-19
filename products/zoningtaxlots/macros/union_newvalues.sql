{% macro union_newvalues(left='new_version', right='prev_version', cols=[]) %}

{%- for col in cols %}
    SELECT
        '{{col}}' AS field,
        COUNT(*) AS count
    FROM {{left}} AS a
    INNER JOIN {{right}} AS b
        ON a.bbl=b.bbl
    WHERE a.{{col}} IS NOT NULL AND b.{{col}} IS NULL
    {% if not loop.last -%}
    UNION ALL
    {%- endif %}
{%- endfor -%}

{% endmacro %}
