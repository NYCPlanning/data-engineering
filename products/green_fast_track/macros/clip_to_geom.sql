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
    -- duckdb has no ST_Relate (DE-9IM), so approximate 'T********' (interiors intersect) as
    -- "intersects, but not merely touching at the boundary" -- excludes edge-only touches
    -- the same way the postgis version did
    INNER JOIN {{ right }} ON (
        ST_INTERSECTS({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }})
        AND NOT ST_TOUCHES({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }})
    )

{%- endmacro %}
