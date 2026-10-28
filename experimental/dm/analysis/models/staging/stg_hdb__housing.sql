SELECT
    job_number,
    comunitydist::integer AS borocd,
    complete_year::integer AS complete_year,
    complete_qrtr,
    classa_net::integer AS classa_net
FROM {{ source('sources', 'dcp_housing') }}
