-- Every geography must land in exactly one legend row, or the app draws it
-- unstyled. This is not hypothetical: units_2025 goes negative for the park and
-- airport NTAs that pick up demolition jobs from their neighbors (MN0191 is at
-- -115), and the old hand-maintained template had no Net loss class for
-- "latest" at all.
--
-- The assignment rule is part of the contract with the app. NTA and CD classes
-- share boundaries, so upper-inclusive is what keeps them disjoint:
--   Net loss   units < 0
--   class 1    0 <= units <= rangeMax     (the class whose rangeMin is 0)
--   class 2-5  rangeMin < units <= rangeMax
-- Boroughs are not classified: each has its own row, matched on geoName, with
-- rangeMin and rangeMax both equal to its value.

{%- set latest = var('cpp_latest_complete_year') | int -%}
{%- set past_col = 'completed_units_' ~ (latest - 9) ~ '_' ~ latest -%}
{%- set proj_col = 'projected_completed_units_' ~ (latest + 1) ~ '_' ~ (latest + 10) -%}
{%- set ranges = [('past', past_col), ('latest', 'units_' ~ latest), ('projected', proj_col)] -%}
{%- set models = [
    ('nta', 'cpp_housing_growth_nta', 'NULL'),
    ('cd', 'cpp_housing_growth_cd', 'NULL'),
    ('boro', 'cpp_housing_growth_boro', 'borough_name')
] %}

WITH measures AS (
{%- for geo, model, name_col in models %}
{%- for rng, col in ranges %}
    SELECT
        '{{ geo }}' AS geo,
        '{{ rng }}' AS "range",
        geography_id,
        {{ name_col }}::text AS geo_name,
        {{ col }} AS units
    FROM {{ ref(model) }}
    {%- if not (loop.last and geo == 'boro') %}
    UNION ALL
    {%- endif %}
{%- endfor %}
{%- endfor %}
),

assigned AS (
    SELECT
        m.geo,
        m."range",
        m.geography_id,
        m.units,
        count(l."rangeLabel") AS layer_rows
    FROM measures AS m
    LEFT JOIN {{ ref('cpp_housing_growth_layers') }} AS l
        ON
            m.geo = l.geo
            AND m."range" = l."range"
            AND CASE
                WHEN m.geo = 'boro'
                    THEN
                        l."geoName" = m.geo_name
                        AND l."rangeMin" = m.units
                        AND l."rangeMax" = m.units
                WHEN l."rangeLabel" = 'Net loss' THEN m.units < 0
                WHEN l."rangeMin" = 0 THEN m.units >= 0 AND m.units <= l."rangeMax"
                ELSE m.units > l."rangeMin" AND m.units <= l."rangeMax"
            END
    GROUP BY m.geo, m."range", m.geography_id, m.units
)

SELECT geo, "range", geography_id, units, layer_rows
FROM assigned
WHERE layer_rows != 1
