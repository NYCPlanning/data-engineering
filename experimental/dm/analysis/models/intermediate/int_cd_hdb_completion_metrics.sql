-- Same sum and filters as KPDB's cpp_housing_growth macro. The 2010Q2-2020Q1 window matches
-- the April 1 census dates to the quarter, so it lines up with the census change.
SELECT
    borocd,
    sum(classa_net) FILTER (
        WHERE complete_qrtr BETWEEN '2010Q2' AND '2020Q1'
    ) AS hdb_net_completed_units_2010q2_2020q1,
    sum(classa_net) FILTER (
        WHERE complete_year BETWEEN 2015 AND 2024
    ) AS hdb_net_completed_units_2015_2024
FROM {{ ref('stg_hdb__housing') }}
WHERE borocd IS NOT NULL
GROUP BY borocd
