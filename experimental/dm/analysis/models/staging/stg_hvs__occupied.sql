{%- for year in [2021, 2023] %}
SELECT
    {{ year }} AS survey_year,
    "CONTROL" AS control,
    "HHSIZE" AS household_size,
    "FW" AS household_weight,
    {%- for r in range(1, 81) %}
    "FW{{ r }}" AS replicate_weight_{{ r }}{{ "," if not loop.last }}
    {%- endfor %}
FROM {{ source('sources', 'hvs_occupied_' ~ year) }}
{% if not loop.last %}UNION ALL{% endif %}
{%- endfor %}
