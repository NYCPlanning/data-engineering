{% macro not_equal_or_null(left_table, right_table, column) %}
    sum(
        (
            {{left_table}}.{{column}} != {{right_table}}.{{column}}
            OR
            ({{left_table}}.{{column}} IS NULL AND {{right_table}}.{{column}} IS NOT NULL)
        )::integer
    )
{% endmacro %}
