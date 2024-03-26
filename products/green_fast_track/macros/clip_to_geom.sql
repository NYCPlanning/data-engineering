/* 
This macro can be used with dbt models, source tables, and CTEs.
The geometry column in the output is named "geom"

For CTEs, must include "left_columns" parameter. The CTE name should be in quotes

If you include a geometry column in "left_columns" parameter, then you will have both original geom column 
from "left" table and the resulting geom column named "geom"
*/

{% macro clip_to_geom(left, right=ref("stg__nyc_boundary"), left_by="geom", right_by="geom", left_columns=[]) %}

    SELECT
        {% if left_columns == [] %}
            {{ dbt_utils.star(from=left, except=[left_by]) }},
        {% else %}
            {% for column in left_columns %}
                {{ left }}.{{ column }},
            {% endfor %}
        {% endif %}
        ST_INTERSECTION({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }}) AS geom
    FROM {{ left }}
    INNER JOIN {{ right }} ON ST_INTERSECTS({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }})

{% endmacro %}
