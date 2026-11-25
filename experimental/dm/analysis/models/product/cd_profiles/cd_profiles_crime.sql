-- Matches the CD Profiles app's columns and the old build's totals: one row per community district, a borough
-- total that also counts its joint interest areas, and a citywide total that counts every complaint, including
-- those in no district.
WITH districts AS (
    SELECT
        borocd,
        is_joint_interest_area,
        major_felony_complaints_2025 AS crime_count,
        sum(major_felony_complaints_2025) OVER (PARTITION BY boro_code) AS crime_count_boro
    FROM {{ ref('dim_community_district') }}
)

SELECT
    borocd,
    crime_count,
    crime_count_boro,
    (
        SELECT count(*) FROM {{ ref('fact_major_felony_complaint') }}
        WHERE year(occurred_date) = 2025
    ) AS crime_count_nyc
FROM districts
WHERE NOT is_joint_interest_area
ORDER BY borocd
