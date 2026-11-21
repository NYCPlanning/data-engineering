SELECT
    pjct_name AS project_name,
    sip_year::INTEGER AS sip_year,
    end_date::DATE AS end_date,
    geom
FROM {{ source('sources', 'vzv_sip_intersections') }}
