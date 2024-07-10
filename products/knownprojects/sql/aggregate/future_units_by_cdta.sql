WITH project_records AS (
    SELECT * FROM {{ ref('longform_cdta_output') }}
),

cdta_details AS (
    SELECT * FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.dcp_cdta2020 -- noqa: PRS, TMP
),

project_records_with_cdta_details AS (
    SELECT
        project_records.*,
        cdta_details.geometry AS cdta_geometry
    FROM project_records
    LEFT JOIN
        cdta_details ON
        project_records.cdta = cdta_details.cdta2020
),

future_units_with_cdta AS (
    SELECT
        cdta,
        record_id,
        units_gross,
        units_net,
        units_net_in_cdta,
        future_phased_units_total_in_cdta,
        future_units_without_phasing_in_cdta,
        completed_units_in_cdta,
        coalesce(within_5_years_in_cdta, 0) AS within_5_years_in_cdta,
        coalesce(from_5_to_10_years_in_cdta, 0) AS from_5_to_10_years_in_cdta,
        coalesce(after_10_years_in_cdta, 0) AS after_10_years_in_cdta,
        cdta_geometry
    FROM project_records_with_cdta_details
    WHERE units_net != 0
),

future_units_by_cdta AS (
    SELECT
        cdta,
        count(record_id) AS records_in_cdta,
        sum(units_gross) AS units_gross,
        sum(units_net_in_cdta) AS units_net,
        sum(future_phased_units_total_in_cdta) AS future_phased_units_total,
        sum(future_units_without_phasing_in_cdta) AS future_units_without_phasing,
        sum(completed_units_in_cdta) AS completed_units,
        sum(within_5_years_in_cdta) AS within_5_years,
        sum(from_5_to_10_years_in_cdta) AS from_5_to_10_years,
        sum(after_10_years_in_cdta) AS after_10_years,
        cdta_geometry
    FROM future_units_with_cdta
    GROUP BY cdta, cdta_geometry
)

SELECT * FROM future_units_by_cdta
