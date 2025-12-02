{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid', 'boroughcode']},
    ]
) }}
WITH sedat AS (
    SELECT * FROM {{ ref("stg__segment_sedat") }}
),
agg AS (
    SELECT
        segmentid,
        lionkey,
        left(lionkey, 1) AS boroughcode,
        array_agg(side) AS sedat_sides
    FROM sedat
    GROUP BY segmentid, lionkey
)
SELECT
    *,
    CASE
        -- staging table has validation that only valid values are '1' and '2'
        -- cases are then both, all 1, all 2, or none
        WHEN ARRAY['1', '2'] <@ sedat_sides THEN 'B'
        WHEN '1' = any(sedat_sides) THEN 'L'
        WHEN '2' = any(sedat_sides) THEN 'R'
    END AS split_election_district_flag
FROM agg
