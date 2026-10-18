{{
    config(
        meta = {
            'description': '''
                Every *_label column in lift_supplemented_map is what the map displays in place
                of a raw column, so it must never be null. A null shows up in a map widget as a
                blank bar, or in a pop-up as the word "null".
            ''',
            'next_steps': '''
                Add a fallback label to the label expression in lift_supplemented_map that says
                why the value is missing.
            '''
        }
    )
}}

{%- set label_columns = [] %}
{%- if execute %}
    {%- for col in adapter.get_columns_in_relation(ref('lift_supplemented_map')) %}
        {%- if col.name.endswith('_label') %}
            {%- do label_columns.append(col.name) %}
        {%- endif %}
    {%- endfor %}
{%- endif %}

SELECT bbl
FROM {{ ref('lift_supplemented_map') }}
WHERE
    FALSE
    {%- for col in label_columns %}
    OR {{ col }} IS NULL
    {%- endfor %}
