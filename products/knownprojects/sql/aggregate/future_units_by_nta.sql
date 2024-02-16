WITH project_records AS (
    SELECT * FROM {{ ref('longform_nta_output') }}
),

nta_details AS (
    SELECT * FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.dcp_nta2020 -- noqa: PRS, TMP
),

project_records_with_nta_details AS (
    SELECT
        project_records.*,
        nta_details.geometry AS nta_geometry
    FROM project_records
    LEFT JOIN
        nta_details ON
        project_records.nta = nta_details.nta2020
),

future_units_with_nta AS (
    SELECT
        nta,
        record_id,
        units_gross,
        units_net,
        units_net_in_nta,
        future_units_without_phasing_in_nta,
        coalesce(within_5_years_in_nta, 0) AS within_5_years_in_nta,
        coalesce(from_5_to_10_years_in_nta, 0) AS from_5_to_10_years_in_nta,
        coalesce(after_10_years_in_nta, 0) AS after_10_years_in_nta,
        nta_geometry
    FROM project_records_with_nta_details
    WHERE units_net != 0
),

future_units_by_nta AS (
    SELECT
        nta,
        count(record_id) AS records_in_nta,
        sum(units_gross) AS units_gross,
        sum(units_net_in_nta) AS units_net,
        sum(future_units_without_phasing_in_nta) AS future_units_without_phasing,
        sum(within_5_years_in_nta) AS within_5_years,
        sum(from_5_to_10_years_in_nta) AS from_5_to_10_years,
        sum(after_10_years_in_nta) AS after_10_years,
        nta_geometry
    FROM future_units_with_nta
    GROUP BY nta, nta_geometry
)

SELECT * FROM future_units_by_nta
