{#
    A log__* model's message text: FORMAT(template, args...), each arg cast to VARCHAR
    with NULL printed as ''. Write the template as plain text (single quotes need no
    escaping); args are SQL expressions. log.sql prefixes the result with "[<id>] ".

        {{ log_message("The {} feature with a GlobalID = {} has no node.", ["source_table", "globalid"]) }}
#}
{% macro log_message(template, args=[]) -%}
    FORMAT(
        '{{ template | replace("'", "''") }}'
        {%- for arg in args %},
        COALESCE(({{ arg }})::VARCHAR, ''){% endfor %}
    )
{%- endmacro %}
