{#-
KPDB's summaries only have the combined 2025-2034 projection, so the two phases are summed from
its records, which are already split by geography.
-#}
{% macro kpdb_projection_metrics(records, summary, key, suffix) -%}
WITH phases AS (
    SELECT
        {{ key }},
        round(sum(within_5_years_in_{{ suffix }})) AS projected_net_units_2025_2029,
        round(sum(from_5_to_10_years_in_{{ suffix }})) AS projected_net_units_2030_2034
    FROM {{ ref(records) }}
    GROUP BY {{ key }}
)

SELECT
    summary.{{ key }},
    summary.projected_completed_units_2025_2034 AS projected_net_units_2025_2034,
    coalesce(phases.projected_net_units_2025_2029, 0) AS projected_net_units_2025_2029,
    coalesce(phases.projected_net_units_2030_2034, 0) AS projected_net_units_2030_2034
FROM {{ ref(summary) }} AS summary
LEFT JOIN phases ON summary.{{ key }} = phases.{{ key }}
{%- endmacro %}
