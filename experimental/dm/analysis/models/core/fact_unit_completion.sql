SELECT
    job_number,
    job_type,
    borocd,
    date_complete,
    complete_year,
    classa_net,
    is_inactive
FROM {{ ref('int_unit_completion_spine') }}
