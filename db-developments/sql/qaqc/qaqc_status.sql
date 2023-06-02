/** QAQC
    inactive_with_update
    partially_complete
**/

DROP TABLE IF EXISTS STATUS_qaqc;
WITH 
JOBNUMBER_inactive_update AS(
    SELECT job_number
    FROM STATUS_devdb
    WHERE date_lastupdt > :'CAPTURE_DATE_PREV'::date
    AND job_number IN (SELECT job_number
        FROM _manual_corrections
        WHERE field = 'job_inactive'
        AND new_value ~* 'Inactive')
),
JOBNUMBER_partially_complete AS(
    SELECT job_number
    FROM STATUS_devdb
    WHERE job_status = '4. Partially Completed Construction'
)

SELECT a.*,
    (CASE 
	 	WHEN a.job_number IN (SELECT job_number FROM JOBNUMBER_inactive_update) THEN 1
	 	ELSE 0
	END) as inactive_with_update,
    (CASE 
	 	WHEN a.job_number IN (SELECT job_number FROM JOBNUMBER_partially_complete) THEN 1
	 	ELSE 0
	END) as partially_complete
INTO STATUS_qaqc
FROM UNITS_qaqc a;