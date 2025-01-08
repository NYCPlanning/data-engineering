{% macro bool_to_str(val, t="'Yes'", f="'No'") %}
    CASE
        WHEN {{ val }} THEN {{ t }}
        ELSE {{ f }}
    END
{% endmacro %}
