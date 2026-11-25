-- A month in more than one report takes the one published later: the 2025 file over the 2024 file (August
-- 2024), the May 2017 archive file over the March 2017 one (2016), and July 2020's new sheet over its old one.
-- They agree on every value except 26th Ward's April 2016 flow (42 MGD in the later file, 45 in the earlier).
{%- set superseded_files = [
    'dep_wastewater_performance_2024.xlsx',
    'dep_wastewater_performance_2017_2022.zip/ARCHIVED_Monthly_Operating_Efficiency_03152017.xlsx',
] %}
SELECT
    * EXCLUDE (sheet_version),
    count(*) OVER (PARTITION BY wastewater_facility_id, report_month) AS reports_with_month
FROM {{ ref('stg_dep__wastewater_performance') }}
QUALIFY row_number() OVER (
    PARTITION BY wastewater_facility_id, report_month
    ORDER BY source_file IN ('{{ superseded_files | join("', '") }}') OR sheet_version = 'old'
) = 1
