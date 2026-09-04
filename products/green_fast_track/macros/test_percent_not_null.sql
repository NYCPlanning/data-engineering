-- fails if more than `threshold` fraction of column_name's rows are null (0.01 = 1%)

{% test percent_not_null(model, column_name, threshold) %}

WITH validation AS (
    SELECT
        CASE WHEN {{ column_name }} IS NULL THEN 1 ELSE 0 END AS is_null
    FROM {{ model }}
),

summary AS (
    SELECT AVG(is_null) AS null_fraction
    FROM validation
)

SELECT *
FROM summary
WHERE null_fraction > {{ threshold }}

{% endtest %}
