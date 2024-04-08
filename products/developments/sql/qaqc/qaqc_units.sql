/** QAQC
    b_large_alt_reduction
    outlier_nb_500plus
    outlier_demo_20plus
    outlier_top_alt_increase
**/

DROP TABLE IF EXISTS units_qaqc;
WITH

jobnumber_large_alt AS (
    SELECT job_number
    FROM units_devdb
    WHERE
        job_type = 'Alteration'
        AND classa_net::numeric < -5
),

jobnumber_large_nb AS (
    SELECT job_number
    FROM units_devdb
    WHERE
        job_type = 'New Building'
        AND classa_prop::numeric > 499
),

jobnumber_large_demo AS (
    SELECT job_number
    FROM units_devdb
    WHERE
        job_type = 'Demolition'
        AND classa_init::numeric > 19
),

jobnumber_top_alt_inc AS (
    SELECT job_number
    FROM units_devdb
    WHERE
        job_type = 'Alteration'
        AND classa_net IS NOT NULL
    ORDER BY classa_net DESC
    LIMIT 20
),

jobnumber_top_alt_dec AS (
    SELECT job_number
    FROM units_devdb
    WHERE
        job_type = 'Alteration'
        AND classa_net IS NOT NULL
    ORDER BY classa_net ASC
    LIMIT 20
)

SELECT
    a.*,
    (CASE
        WHEN a.job_number IN (SELECT job_number FROM jobnumber_large_alt) THEN 1
        ELSE 0
    END) AS b_large_alt_reduction,
    (CASE
        WHEN a.job_number IN (SELECT job_number FROM jobnumber_large_nb) THEN 1
        ELSE 0
    END) AS outlier_nb_500plus,
    (CASE
        WHEN a.job_number IN (SELECT job_number FROM jobnumber_large_demo) THEN 1
        ELSE 0
    END) AS outlier_demo_20plus,
    (CASE
        WHEN a.job_number IN (SELECT job_number FROM jobnumber_top_alt_inc) THEN 1
        ELSE 0
    END) AS outlier_top_alt_increase

INTO units_qaqc
FROM _init_qaqc AS a;
