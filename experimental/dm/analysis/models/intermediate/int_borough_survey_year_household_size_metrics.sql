-- Average household size in buildings built before 2010 and 2010 or later, by borough and HVS
-- survey year, with standard errors. HPD's replication estimator: compute the estimate with the
-- household weight and with each of the 80 replicate weights, then
--   SE = sqrt(4 / 80 x sum of (replicate estimate - estimate)^2)
WITH weights AS (
    SELECT survey_year, boro_code, is_built_2010_or_later, household_size, 0 AS replicate, household_weight AS weight
    FROM {{ ref('int_housing_survey_household_spine') }}
    UNION ALL
    SELECT
        survey_year, boro_code, is_built_2010_or_later, household_size,
        replace(replicate_name, 'replicate_weight_', '')::integer AS replicate,
        weight
    FROM (
        UNPIVOT (
            SELECT * EXCLUDE (control, household_weight)
            FROM {{ ref('int_housing_survey_household_spine') }}
        )
        ON COLUMNS('^replicate_weight_')
        INTO NAME replicate_name VALUE weight
    )
),

estimates AS (
    SELECT
        survey_year,
        boro_code,
        replicate,
        count(*) FILTER (NOT is_built_2010_or_later) AS records_built_before_2010,
        count(*) FILTER (is_built_2010_or_later) AS records_built_2010_or_later,
        sum(weight * household_size) FILTER (NOT is_built_2010_or_later)
        / sum(weight) FILTER (NOT is_built_2010_or_later) AS size_built_before_2010,
        sum(weight * household_size) FILTER (is_built_2010_or_later)
        / sum(weight) FILTER (is_built_2010_or_later) AS size_built_2010_or_later
    FROM weights
    GROUP BY ALL
),

with_gap AS (
    SELECT *, size_built_2010_or_later - size_built_before_2010 AS size_gap
    FROM estimates
)

SELECT
    estimate.survey_year,
    estimate.boro_code,
    estimate.records_built_before_2010 AS hvs_records_built_before_2010,
    estimate.records_built_2010_or_later AS hvs_records_built_2010_or_later,
    {%- for measure, column in [
        ('size_built_before_2010', 'average_household_size_built_before_2010_hvs'),
        ('size_built_2010_or_later', 'average_household_size_built_2010_or_later_hvs'),
        ('size_gap', 'household_size_gap_built_2010_or_later_hvs'),
    ] %}
    round(estimate.{{ measure }}, 2) AS {{ column }},
    round(sqrt(4 / 80 * sum((replicate.{{ measure }} - estimate.{{ measure }}) ^ 2)), 3) AS {{ column }}_se
    {{- "," if not loop.last }}
    {%- endfor %}
FROM with_gap AS estimate
INNER JOIN with_gap AS replicate
    ON estimate.survey_year = replicate.survey_year
    AND estimate.boro_code = replicate.boro_code
    AND replicate.replicate > 0
WHERE estimate.replicate = 0
GROUP BY ALL
