{{ config(
    materialized = 'table'
) }}

WITH completed_construction_records AS (
    SELECT
        bbl,
        bin,
        units_co,
        CASE
            WHEN classa_prop IS NULL AND job_status = '5. Completed Construction' THEN 0
            ELSE classa_prop
        END AS classa_prop_modified,
        job_type,
        job_status,
        date_complete,
        row_number() OVER (
            PARTITION BY bbl, bin
            ORDER BY date_complete DESC
        ) AS order_num
    FROM {{ ref("stg__dcp_developments") }}
    WHERE job_status IN ('5. Completed Construction', '4. Partially Completed Construction')
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
