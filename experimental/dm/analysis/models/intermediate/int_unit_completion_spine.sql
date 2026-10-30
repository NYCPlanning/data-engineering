-- DevDB only sets date_complete once a job's units count as built or removed.
SELECT
    job_number,
    job_type,
    borocd,
    date_complete,
    complete_year,
    classa_net,
    is_inactive
FROM {{ ref('stg_hdb__housing') }}
WHERE date_complete IS NOT NULL
