SELECT
    job_number,
    job_type,
    comunitydist::integer AS borocd,
    complete_year::integer AS complete_year,
    date_complete::date AS date_complete,
    job_inactive IS NOT NULL AS is_inactive,
    classa_net::integer AS classa_net
FROM {{ source('sources', 'dcp_housing') }}
