-- tests column values to have 2263 projection (state plane)

{% test is_epsg_2263(model, column_name) %}

WITH validation AS (
    SELECT
        {{ column_name }} AS geom_column
    FROM {{ model }}
),

validation_errors AS (
    SELECT
        geom_column
    FROM validation
    WHERE ST_SRID(geom_column) != 2263

)

SELECT *
FROM validation_errors

{% endtest %}
