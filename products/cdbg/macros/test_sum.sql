{% test sum_by(model, group_by, target_column, val, precision=4) %}

SELECT
    {{ group_by }}, sum({{ target_column }}) AS sum, array_agg( {{target_column }}) AS vals
FROM {{ model }}
GROUP BY {{ group_by }}
HAVING round(sum({{ target_column }})::numeric, {{ precision }}) <> {{ val }}

{% endtest %}
