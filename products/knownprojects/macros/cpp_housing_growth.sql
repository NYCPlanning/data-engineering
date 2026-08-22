{#
    Housing growth for one geography, for the Capital Projects Portal.

    Parameters:
      geography_type   dcp_censusdata geotype supplying the 2020 baseline
      kpdb_model       KPDB aggregation carrying the phased future units
      kpdb_geo_column  its geography column
      housing_column   the matching column on dcp_housing

    Both completed_units_* columns are dcp_housing's classa_net summed over a
    window, so they are net of demolitions and unit losses rather than gross
    completions. The name follows the column AE specified.

    The 2020 Census counts housing as of April 1 2020, so completions in 2020Q1
    are already in the baseline and are excluded from the change since then.
    Column years are literal by request; the projection window is anchored to
    PHASING_ANCHOR_YEAR in recipe.yml, since KPDB's phasing is a proportion
    relative to the data vintage rather than a calendar date.
#}

{% macro cpp_housing_growth(geography_type, kpdb_model, kpdb_geo_column, housing_column) %}

WITH baseline AS (
    SELECT
        geography_id,
        units_2020
    FROM {{ ref('census_housing_units_2020') }}
    WHERE geography_type = '{{ geography_type }}'
),

completions AS (
    SELECT
        {{ housing_column }}::text AS geography_id,
        sum(classa_net::numeric) FILTER (
            WHERE complete_year::numeric BETWEEN 2015 AND 2025
        ) AS completed_units_2015_2025,
        -- The census counts housing as of April 1 2020, so 2020Q1 completions
        -- are already in units_2020. Counting them again would double them.
        sum(classa_net::numeric) FILTER (
            WHERE complete_qrtr > '2020Q1' AND complete_year::numeric <= 2025
        ) AS net_units_since_2020
    FROM {{ source('recipe_sources', 'dcp_housing') }}
    WHERE {{ housing_column }} IS NOT NULL
    GROUP BY {{ housing_column }}::text
),

-- Reads low for fine-grained geographies: KPDB rounds each project's units
-- once per geography it touches, so a project split across 262 NTAs loses more
-- to rounding than the same project split across 71 community districts. The
-- two outputs' citywide projections differ by ~1% for that reason.
projected AS (
    SELECT
        {{ kpdb_geo_column }}::text AS geography_id,
        coalesce(within_5_years, 0)
        + coalesce(from_5_to_10_years, 0) AS projected_completed_units_2025_2035
    FROM {{ ref(kpdb_model) }}
),

joined AS (
    SELECT
        a.geography_id,
        a.units_2020,
        coalesce(b.completed_units_2015_2025, 0) AS completed_units_2015_2025,
        coalesce(b.net_units_since_2020, 0) AS completed_units_2020_2025,
        a.units_2020
        + coalesce(b.net_units_since_2020, 0) AS units_2025,
        coalesce(
            c.projected_completed_units_2025_2035, 0
        ) AS projected_completed_units_2025_2035
    FROM baseline AS a
    LEFT JOIN completions AS b ON b.geography_id = a.geography_id
    LEFT JOIN projected AS c ON c.geography_id = a.geography_id
)

SELECT
    geography_id,
    units_2020,
    completed_units_2015_2025,
    completed_units_2020_2025,
    units_2025,
    projected_completed_units_2025_2035,
    units_2025 + projected_completed_units_2025_2035 AS projected_units_2035
FROM joined
ORDER BY geography_id ASC

{% endmacro %}
