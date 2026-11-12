{%- for year in [2021, 2023] %}
SELECT
    {{ year }} AS survey_year,
    "CONTROL" AS control,
    -- HVS numbers boroughs differently from DCP
    CASE "BORO" WHEN 1 THEN 2 WHEN 2 THEN 3 WHEN 3 THEN 1 WHEN 4 THEN 4 WHEN 5 THEN 5 END AS boro_code,
    "OCC" = 1 AS is_occupied,
    -- the last year built bin is "2010 or later"
    "YEARBUILT" = 11 AS is_built_2010_or_later
FROM {{ source('sources', 'hvs_allunits_' ~ year) }}
{% if not loop.last %}UNION ALL{% endif %}
{%- endfor %}
