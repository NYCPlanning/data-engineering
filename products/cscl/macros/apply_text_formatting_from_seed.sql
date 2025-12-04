/*
Given a seed file with fields
- field_name
- field_length
- justify_and_fill
- blank_if_none

Applies text formatting using pg function FORMAT_LION_TEXT to each field in the seed file.
*/

{% macro apply_text_formatting_from_seed(formatting_seed) -%}

{%- set query -%}
    SELECT
        field_name,
        field_length,
        LEFT(justify_and_fill,2) = 'LJ' AS left_justified,
        CASE 
            WHEN RIGHT(justify_and_fill,2) = 'ZF' THEN '''0'''
            ELSE ''' '''
        END AS fill,
        blank_if_none
    FROM {{ ref(formatting_seed) }}
{%- endset -%}

{%- set fields = run_query(query) -%}

{%- if execute -%}
    {% for field in fields -%}
        FORMAT_LION_TEXT(
            {{ "''" if field['field_name'].startswith('filler_') else field['field_name'] }}::TEXT,
            {{ field['field_length'] }},
            {{ field['fill'] }},
            {{ field['blank_if_none'] }},
            {{ field['left_justified'] }}
        ) AS {{ field['field_name'] }}
        {%- if not loop.last -%},{% endif %}
    {% endfor -%}
{%- endif -%}

{%- endmacro %}
