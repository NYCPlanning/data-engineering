{% macro format_text(column, fill, n, left=false, blank_if_none=true) %}

CASE
    WHEN {{ column }} IS NULL THEN
        {% if blank_if_none %}
            rpad('', n, ' ')
        {% else %}
            rpad('', n, fill)
        {% endif %}
    ELSE
        {%- if left %}
            rpad( -- left justified -> pad on right
        {% else %}
            lpad(
        {% endif %}
                {{ column }}::TEXT,
                {{ n }},
                {{ fill }}
            )

{% endmacro %}
