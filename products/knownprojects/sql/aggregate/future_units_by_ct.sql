WITH project_records AS (
    SELECT * FROM {{ ref('longform_ct_output') }}
),

ct_details AS (
    SELECT * FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.dcp_ct2020_wi -- noqa: PRS, TMP
),

project_records_with_ct_details AS (
    SELECT
        project_records.*,
        ct_details.geometry AS ct_geometry
    FROM project_records
    LEFT JOIN
        ct_details ON
        project_records.ct = ct_details.boroct2020
),

future_units_with_ct AS (
    SELECT
        ct,
        record_id,
        units_gross,
        units_net,
        units_net_in_ct,
        future_units_without_phasing_in_ct,
        coalesce(within_5_years_in_ct, 0) AS within_5_years_in_ct,
        coalesce(from_5_to_10_years_in_ct, 0) AS from_5_to_10_years_in_ct,
        coalesce(after_10_years_in_ct, 0) AS after_10_years_in_ct,
        ct_geometry
    FROM project_records_with_ct_details
    WHERE units_net != 0
),

future_units_by_ct AS (
    SELECT
        ct,
        count(record_id) AS records_in_ct,
        sum(units_gross) AS units_gross,
        sum(units_net_in_ct) AS units_net,
        sum(future_units_without_phasing_in_ct) AS future_units_without_phasing,
        sum(within_5_years_in_ct) AS within_5_years,
        sum(from_5_to_10_years_in_ct) AS from_5_to_10_years,
        sum(after_10_years_in_ct) AS after_10_years,
        ct_geometry
    FROM future_units_with_ct
    GROUP BY ct, ct_geometry
)

SELECT * FROM future_units_by_ct
