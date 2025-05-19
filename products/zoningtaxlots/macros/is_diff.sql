{% macro is_diff(left_table, right_table, column) %}
    (
        {{left_table}}.{{column}} != {{right_table}}.{{column}} 
        OR 
        ({{left_table}}.{{column}} IS NULL AND {{right_table}}.{{column}} IS NOT NULL)
        OR
        ({{left_table}}.{{column}} IS NOT NULL AND {{right_table}}.{{column}} IS NULL)
    )
{% endmacro %}
