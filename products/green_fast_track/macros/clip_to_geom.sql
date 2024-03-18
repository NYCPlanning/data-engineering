{% macro clip_to_geom(left, right=ref("stg__nyc_boundary"), left_by="geom", right_by="geom") %}

    SELECT
        {{ dbt_utils.star(from=left, except=[left_by]) }},
        ST_INTERSECTION({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }}) AS geom
    FROM {{ left }}
    INNER JOIN {{ right }} ON ST_INTERSECTS({{ left }}.{{ left_by }}, {{ right }}.{{ right_by }})

{% endmacro %}
