{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Assign historic district and landmark designations from LPC data
-- Historic district: first alphabetical district per BBL
-- Landmark: type determination based on number of landmark types per BBL

WITH base_pluto AS (
    SELECT
        bbl,
        borocode,
        block,
        lot
    FROM {{ target.schema }}.pluto
),

histdistricts AS (
    SELECT
        bbl,
        hist_dist
    FROM (
        SELECT
            bbl,
            hist_dist,
            ROW_NUMBER() OVER (
                PARTITION BY bbl
                ORDER BY hist_dist
            ) AS row_number
        FROM {{ ref('stg__lpc_historic_districts') }}
        WHERE
            hist_dist != '0'
            AND hist_dist NOT LIKE 'Individual Landmark%'
    ) AS x
    WHERE x.row_number = 1
),

landmarks AS (
    SELECT DISTINCT
        bbl,
        lm_type,
        ROW_NUMBER() OVER (
            PARTITION BY bbl
            ORDER BY lm_type
        ) AS row_number
    FROM (
        SELECT DISTINCT
            bbl,
            lm_type
        FROM {{ ref('stg__lpc_landmarks') }}
        WHERE
            (lm_type = 'Interior Landmark' OR lm_type = 'Individual Landmark')
            AND status = 'DESIGNATED'
            AND most_curre = '1'
            AND (
                last_actio = 'DESIGNATED'
                OR last_actio = 'DESIGNATED (AMENDMENT/MODIFICATION ACCEPTED)'
            )
    ) AS x
),

maxnum AS (
    SELECT
        bbl,
        MAX(row_number) AS maxrow_number
    FROM landmarks
    GROUP BY bbl
),

landmark_types AS (
    SELECT
        l.bbl,
        CASE
            WHEN m.maxrow_number = 1 THEN UPPER(l.lm_type)
            WHEN m.maxrow_number = 2 THEN UPPER('Individual and Interior Landmark')
            ELSE UPPER(l.lm_type)
        END AS landmark
    FROM landmarks AS l
    INNER JOIN maxnum AS m ON l.bbl = m.bbl
    WHERE l.row_number = 1
)

SELECT
    p.bbl,
    h.hist_dist AS histdist,
    lt.landmark
FROM base_pluto AS p
LEFT JOIN histdistricts AS h
    ON p.borocode || LPAD(p.block, 5, '0') || LPAD(p.lot, 4, '0') = h.bbl
LEFT JOIN landmark_types AS lt
    ON p.borocode || LPAD(p.block, 5, '0') || LPAD(p.lot, 4, '0') = lt.bbl
