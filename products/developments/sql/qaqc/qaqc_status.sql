/** QAQC
    inactive_with_update
    partially_complete
**/

DROP TABLE IF EXISTS status_qaqc;
WITH
jobnumber_inactive_update AS (
    SELECT job_number
    FROM status_devdb
    WHERE
        date_lastupdt > :'CAPTURE_DATE_PREV'::date
        AND job_number IN (
            SELECT job_number
            FROM _manual_corrections
            WHERE
                field = 'job_inactive'
                AND new_value ~* 'Inactive'
        )
),
jobnumber_partially_complete AS (
    SELECT job_number
    FROM status_devdb
    WHERE job_status = '4. Partially Completed Construction'
)

SELECT
    a.*,
    (CASE
        WHEN a.job_number IN (SELECT job_number FROM jobnumber_inactive_update) THEN 1
        ELSE 0
    END) AS inactive_with_update,
    (CASE
        WHEN a.job_number IN (SELECT job_number FROM jobnumber_partially_complete) THEN 1
        ELSE 0
    END) AS partially_complete
INTO status_qaqc
FROM units_qaqc AS a;
