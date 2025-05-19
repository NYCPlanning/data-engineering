/* 
This macro can be used with dbt models, source tables, and CTEs.
The geometry column in the output is named "geom"

For CTEs, must include "left_columns" parameter. The CTE name should be in quotes

If you include a geometry column in "left_columns" parameter, then you will have both original geom column 
from "left" table and the resulting geom column named "geom"
*/

{% macro clip_to_geom(left, right=ref("stg__nyc_boundary"), left_by="geom", right_by="geom", left_columns=[]) -%}

    SELECT
        {% if left_columns == [] -%}
            {{ dbt_utils.star(from=left, except=[left_by]) }},
        {% else %}
            {% for column in left_columns %}
                {{ left }}.{{ column }},
            {% endfor %}
        {% endif -%}
        
        -- ST_Intersection is much more costly than ST_CoveredBy
        -- So avoid using intersection when possible
        -- see https://postgis.net/documentation/tips/tip_intersection_faster/
        CASE
            WHEN ST_COVEREDBY({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }})
                THEN {{ left }}.{{ left_by }}
            ELSE ST_INTERSECTION({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }})
        END
         AS geom
    FROM {{ left }}
    -- use ST_Relate rather than ST_Intersect to avoid overlapping edges
    INNER JOIN {{ right }} ON ST_RELATE({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }}, 'T********')

{%- endmacro %}
