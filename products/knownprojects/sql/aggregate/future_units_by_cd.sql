WITH project_records AS (
    SELECT * FROM {{ ref('longform_cd_output') }}
),

cd_details AS (
    SELECT * FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.dcp_cdboundaries_wi -- noqa: PRS, TMP
),

project_records_with_cd_details AS (
    SELECT
        project_records.*,
        cd_details.geometry AS cd_geometry
    FROM project_records
    LEFT JOIN
        cd_details ON
        project_records.cd = cd_details.borocd
),

future_units_with_cd AS (
    SELECT
        cd,
        record_id,
        units_gross,
        units_net,
        units_net_in_cd,
        future_units_without_phasing_in_cd,
        coalesce(within_5_years_in_cd, 0) AS within_5_years_in_cd,
        coalesce(from_5_to_10_years_in_cd, 0) AS from_5_to_10_years_in_cd,
        coalesce(after_10_years_in_cd, 0) AS after_10_years_in_cd,
        cd_geometry
    FROM project_records_with_cd_details
    WHERE units_net != 0
),

future_units_by_cd AS (
    SELECT
        cd,
        count(record_id) AS records_in_cd,
        sum(units_gross) AS units_gross,
        sum(units_net_in_cd) AS units_net,
        sum(future_units_without_phasing_in_cd) AS future_units_without_phasing,
        sum(within_5_years_in_cd) AS within_5_years,
        sum(from_5_to_10_years_in_cd) AS from_5_to_10_years,
        sum(after_10_years_in_cd) AS after_10_years,
        cd_geometry
    FROM future_units_with_cd
    GROUP BY cd, cd_geometry
)

SELECT * FROM future_units_by_cd
