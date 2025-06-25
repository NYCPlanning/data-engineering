DROP TABLE IF EXISTS qaqc_app_additions;
WITH
classa_net_mismatch AS (
    SELECT job_number
    FROM final_devdb
    WHERE
        classa_init IS NOT NULL
        AND classa_prop IS NOT NULL
        AND classa_net != classa_prop - classa_init
)
SELECT
    a.job_number,
    (CASE
        WHEN a.job_number IN (SELECT job_number FROM classa_net_mismatch) THEN 1
        ELSE 0
    END) AS classa_net_mismatch
INTO qaqc_app_additions
FROM final_devdb AS a;

ALTER TABLE qaqc_app_additions ADD manual_hny_match_check int;
WITH manual_hny_match AS (
    SELECT job_number
    FROM hny_corrections
    WHERE
        action = 'add'
        AND
        hny_id IN (SELECT hny_id FROM hny_no_match)
)
UPDATE qaqc_app_additions
SET manual_hny_match_check = (
    CASE
        WHEN qaqc_app_additions.job_number NOT IN (SELECT job_number FROM manual_hny_match) THEN 0
        ELSE 1
    END
);

ALTER TABLE qaqc_app_additions ADD manual_corrections_not_applied int;
UPDATE qaqc_app_additions
SET manual_corrections_not_applied = (
    CASE
        WHEN qaqc_app_additions.job_number IN (SELECT job_number FROM corrections_not_applied) THEN 1 ELSE 0
    END
);

/*
If the projects are residential complete or partially complete status
but the complete_year and complete_quarter are still null. Then it is likely the COs
are missing.
*/
ALTER TABLE qaqc_app_additions ADD complete_year_quarter_null int;
UPDATE qaqc_app_additions
SET complete_year_quarter_null = (
    CASE
        WHEN qaqc_app_additions.job_number IN (
            SELECT job_number
            FROM final_devdb
            WHERE
                job_status IN ('4. Partially Completed Construction', '5. Completed Construction')
                AND (complete_year IS NULL OR complete_qrtr IS NULL)
                AND resid_flag = 'Residential'
        ) THEN 1 ELSE 0
    END
);
