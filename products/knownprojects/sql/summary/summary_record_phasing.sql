-- Contains summary 
DROP TABLE IF EXISTS summary_record_phasing;

WITH kpdb_limited AS (
    SELECT
        coalesce(source, 'NULL_source') AS source,
        coalesce(status, 'NULL_status') AS status,
        has_project_phasing,
        has_future_units,
        prop_within_5_years,
        prop_5_to_10_years,
        prop_after_10_years
    FROM kpdb
),

summary_record_phasing_final AS (
    SELECT
        source,
        status,
        count(*) AS source_status_count,
        sum(CASE WHEN has_project_phasing THEN 1 ELSE 0 END) AS records_with_phasing,
        sum(CASE WHEN has_future_units THEN 1 ELSE 0 END) AS records_with_future_units,
        sum(CASE WHEN nullif(prop_within_5_years, 0) IS NOT null THEN 1 ELSE 0 END) AS records_with_phase_1,
        sum(CASE WHEN nullif(prop_5_to_10_years, 0) IS NOT null THEN 1 ELSE 0 END) AS records_with_phase_2,
        sum(CASE WHEN nullif(prop_after_10_years, 0) IS NOT null THEN 1 ELSE 0 END) AS records_with_phase_3
    FROM kpdb_limited
    GROUP BY DISTINCT ROLLUP (source, status) -- noqa: PRS
)

SELECT
    coalesce(source, 'TOTAL') AS source,
    coalesce(status, 'TOTAL') AS status,
    source_status_count,
    records_with_future_units,
    records_with_phasing,
    source_status_count - records_with_phasing AS records_with_no_phasing,
    records_with_phase_1,
    records_with_phase_2,
    records_with_phase_3
INTO summary_record_phasing
FROM summary_record_phasing_final
ORDER BY source, status;
