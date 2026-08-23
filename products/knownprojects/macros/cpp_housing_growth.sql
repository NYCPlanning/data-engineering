{#
    Housing growth for one geography, for the Capital Projects Portal.

    Parameters:
      geography_type   dcp_censusdata geotype supplying the census baseline
      kpdb_model       KPDB aggregation carrying the phased future units
      kpdb_geo_column  its geography column
      housing_column   the matching column on dcp_housing

    Year-labeled counts are as of the end of that year, so a project completed
    in 2025 counts toward 2025. The census counts housing as of April 1 2020,
    so units_2020 adds the rest of 2020 to it and units_2020_census keeps the
    raw figure.

    Both completed_units_* columns are dcp_housing's classa_net summed over a
    window, so they are net of demolitions and unit losses rather than gross
    completions.
#}

{% macro cpp_housing_growth(
    geography_type,
    kpdb_model,
    kpdb_geo_column,
    housing_column,
    source_model='kpdb_deduplicated'
) %}

WITH baseline AS (
    SELECT
        geography_id,
        units_2020 AS units_2020_census
    FROM {{ ref('census_housing_units_2020') }}
    WHERE geography_type = '{{ geography_type }}'
),

completions AS (
    SELECT
        {{ housing_column }}::text AS geography_id,
        -- the rest of 2020, to carry the census figure to year end
        sum(classa_net::numeric) FILTER (
            WHERE complete_qrtr > '2020Q1' AND complete_year::numeric = 2020
        ) AS completed_units_2020_q2_q4,
        sum(classa_net::numeric) FILTER (
            WHERE complete_year::numeric BETWEEN 2016 AND 2025
        ) AS completed_units_2016_2025,
        sum(classa_net::numeric) FILTER (
            WHERE complete_year::numeric BETWEEN 2021 AND 2025
        ) AS completed_units_2021_2025
    FROM {{ source('recipe_sources', 'dcp_housing') }}
    WHERE {{ housing_column }} IS NOT NULL
    GROUP BY {{ housing_column }}::text
),

projected AS (
    SELECT
        {{ kpdb_geo_column }}::text AS geography_id,
        coalesce(within_5_years, 0)
        + coalesce(from_5_to_10_years, 0) AS projected_completed_units_2026_2035
    FROM {{ ref(kpdb_model) }}
),

joined AS (
    SELECT
        a.geography_id,
        a.units_2020_census,
        a.units_2020_census
        + coalesce(b.completed_units_2020_q2_q4, 0) AS units_2020,
        coalesce(b.completed_units_2016_2025, 0) AS completed_units_2016_2025,
        coalesce(b.completed_units_2021_2025, 0) AS completed_units_2021_2025,
        coalesce(
            c.projected_completed_units_2026_2035, 0
        ) AS projected_completed_units_2026_2035
    FROM baseline AS a
    LEFT JOIN completions AS b ON b.geography_id = a.geography_id
    LEFT JOIN projected AS c ON c.geography_id = a.geography_id
)

SELECT
    geography_id,
    units_2020_census,
    units_2020,
    completed_units_2016_2025,
    completed_units_2021_2025,
    units_2020 + completed_units_2021_2025 AS units_2025,
    projected_completed_units_2026_2035,
    units_2020
    + completed_units_2021_2025
    + projected_completed_units_2026_2035 AS projected_units_2035
FROM joined
ORDER BY geography_id ASC

{% endmacro %}
