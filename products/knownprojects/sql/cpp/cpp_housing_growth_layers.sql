{{ config(materialized='table', tags=['cpp']) }}

-- Map legend for the Capital Projects Portal.
--
-- NTA and CD get five natural-break classes per measure, plus a Net loss class
-- where the measure goes negative. Boroughs are not classified: there are only
-- five, so each takes one step of the ramp ordered by value, which is what the
-- template the app consumes already did.
--
-- Breaks are recomputed every build, so a boundary moves when the values move.
-- Column names are camelCase because that is the shape the app consumes.

{%- set latest = cpp_latest_year() | int -%}
{%- set past_col = 'completed_units_' ~ (latest - 9) ~ '_' ~ latest -%}
{%- set proj_col = 'projected_completed_units_' ~ (latest + 1) ~ '_' ~ (latest + 10) %}

WITH measures AS (
    SELECT 'nta' AS geo, 'past' AS "range", {{ past_col }} AS units
    FROM {{ ref('cpp_housing_growth_nta') }}
    UNION ALL
    SELECT 'nta' AS geo, 'latest' AS "range", units_{{ latest }} AS units
    FROM {{ ref('cpp_housing_growth_nta') }}
    UNION ALL
    SELECT 'nta' AS geo, 'projected' AS "range", {{ proj_col }} AS units
    FROM {{ ref('cpp_housing_growth_nta') }}
    UNION ALL
    SELECT 'cd' AS geo, 'past' AS "range", {{ past_col }} AS units
    FROM {{ ref('cpp_housing_growth_cd') }}
    UNION ALL
    SELECT 'cd' AS geo, 'latest' AS "range", units_{{ latest }} AS units
    FROM {{ ref('cpp_housing_growth_cd') }}
    UNION ALL
    SELECT 'cd' AS geo, 'projected' AS "range", {{ proj_col }} AS units
    FROM {{ ref('cpp_housing_growth_cd') }}
),

{{ jenks_break_ctes('measures', 'breaks') }},

-- one Net loss class per group that has negative values, so every geography
-- gets a class instead of being left unstyled
net_loss AS (
    SELECT
        geo,
        "range",
        6 AS class_id,
        0::double precision AS range_max,
        min(units)::double precision AS range_min
    FROM measures
    WHERE units < 0
    GROUP BY geo, "range"
),

-- five boroughs, five ramp steps: each borough is its own class, ordered by value
boro AS (
    SELECT 'past' AS "range", borough_name, {{ past_col }}::double precision AS units
    FROM {{ ref('cpp_housing_growth_boro') }}
    UNION ALL
    SELECT 'latest' AS "range", borough_name, units_{{ latest }}::double precision AS units
    FROM {{ ref('cpp_housing_growth_boro') }}
    UNION ALL
    SELECT 'projected' AS "range", borough_name, {{ proj_col }}::double precision AS units
    FROM {{ ref('cpp_housing_growth_boro') }}
),

classified AS (
    SELECT
        geo,
        "range",
        class_id,
        range_min,
        range_max
    FROM breaks
    UNION ALL
    SELECT
        geo,
        "range",
        class_id,
        range_min,
        range_max
    FROM net_loss
),

layers AS (
    SELECT
        geo,
        "range",
        class_id,
        range_min,
        range_max,
        NULL::text AS geo_name,
        CASE
            WHEN class_id = 6 THEN 'Net loss'
            ELSE
                trim(to_char(range_min, 'FM999,999,999'))
                || ' - '
                || trim(to_char(range_max, 'FM999,999,999'))
        END AS range_label
    FROM classified
    UNION ALL
    SELECT
        'boro' AS geo,
        "range",
        row_number() OVER (PARTITION BY "range" ORDER BY units ASC) AS class_id,
        units AS range_min,
        units AS range_max,
        borough_name AS geo_name,
        trim(to_char(units, 'FM999,999,999')) AS range_label
    FROM boro
)

SELECT
    a.geo,
    a."range",
    a.geo_name AS "geoName",
    a.range_min::bigint AS "rangeMin",
    a.range_max::bigint AS "rangeMax",
    a.range_label AS "rangeLabel",
    b.color_hex AS "colorHex",
    '['
    || ('x' || substr(b.color_hex, 2, 2))::bit(8)::int || ', '
    || ('x' || substr(b.color_hex, 4, 2))::bit(8)::int || ', '
    || ('x' || substr(b.color_hex, 6, 2))::bit(8)::int
    || ']' AS "colorRgba"
FROM layers AS a
INNER JOIN {{ ref('cpp_housing_growth_colors') }} AS b
    ON a."range" = b."range" AND a.class_id = b.class_id
ORDER BY a.geo ASC, a."range" ASC, a.class_id ASC
