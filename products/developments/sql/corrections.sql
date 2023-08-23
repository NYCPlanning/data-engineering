DROP TABLE IF EXISTS corrections_reference;
WITH 
applied AS (
    SELECT
        a.job_number,
        a.field,
        a.old_value,
        a.new_value,
        a.reason,
        a.edited_date,
        a.editor,
        b.pre_corr_value,
        1 as corr_applied
    FROM _manual_corrections a
    INNER JOIN corrections_applied b
    ON a.job_number = b.job_number
    AND a.field = b.field
    AND (a.old_value = b.old_value OR (a.old_value IS NULL AND b.old_value IS NULL))
    AND (a.new_value = b.new_value OR (a.new_value IS NULL AND b.new_value IS NULL))
),
not_applied AS (
    SELECT
        a.job_number,
        a.field,
        a.old_value,
        a.new_value,
        a.reason,
        a.edited_date,
        a.editor,
        b.pre_corr_value,
        0 as corr_applied
    FROM _manual_corrections a
    INNER JOIN corrections_not_applied b
    ON a.job_number = b.job_number
    AND a.field = b.field
    AND (a.old_value = b.old_value OR (a.old_value IS NULL AND b.old_value IS NULL))
    AND (a.new_value = b.new_value OR (a.new_value IS NULL AND b.new_value IS NULL))
)
SELECT 
    NOW() as build_dt, 
    a.job_number,
    a.field,
    a.old_value,
    a.new_value,
    a.reason,
    a.edited_date,
    a.editor,
    b.pre_corr_value,
    b.corr_applied,
    (a.job_number IN (SELECT job_number FROM FINAL_devdb))::integer as job_in_devdb
INTO corrections_reference
FROM _manual_corrections a
LEFT JOIN (SELECT * FROM applied UNION SELECT * FROM not_applied) b
ON a.job_number = b.job_number
	AND a.field = b.field
	AND (a.old_value = b.old_value OR (a.old_value IS NULL AND b.old_value IS NULL))
	AND (a.new_value = b.new_value OR (a.new_value IS NULL AND b.new_value IS NULL))
;