{{ config(
    materialized = 'table'
) }}

WITH completed_jobs AS (
    SELECT
        *,
        bool_or(classa_prop IS NOT null) OVER (PARTITION BY bbl, bin) AS bin_has_unit_count
    FROM {{ ref("stg__dcp_developments") }}
    WHERE job_status IN ('5. Completed Construction', '4. Partially Completed Construction')
),

completed_construction_records AS (
    SELECT
        bbl,
        bin,
        units_co,
        CASE
            WHEN classa_prop IS null AND job_status = '5. Completed Construction' THEN 0
            ELSE classa_prop
        END AS classa_prop_modified,
        job_type,
        job_status,
        date_complete,
        row_number() OVER (
            PARTITION BY bbl, bin
            ORDER BY date_complete DESC
        ) AS order_num
    FROM completed_jobs
    -- an alteration without a unit count would otherwise read as 0 units and hide the BIN's real count
    WHERE NOT (job_type = 'Alteration' AND classa_prop IS null AND bin_has_unit_count)
),

most_recent_construction_by_bin AS (
    SELECT
        bbl,
        bin,
        units_co,
        classa_prop_modified AS classa_prop,
        job_type,
        job_status,
        date_complete
    FROM completed_construction_records
    WHERE order_num = 1 -- Selecting the most recent record per BIN
)

SELECT
    bbl,
    sum(units_co) AS units_co,
    sum(classa_prop) AS classa_prop,
    count(*) AS count_bins
FROM most_recent_construction_by_bin
WHERE job_type <> 'Demolition' -- Exclude demolished buildings as property taxes may still be paid, aligning with the DOF data
GROUP BY bbl
