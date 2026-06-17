WITH election_districts_with_borough AS (
    SELECT
        ed.globalid,
        ed.electdist AS election_district,
        ed.assembly_district,
        ed.congress_district,
        ed.state_sen_district,
        ed.muni_court_district,
        ed.city_council_district,
        -- Derive borough from atomicpolygons by finding any AP with matching ED+AD
        -- The combination of Election District + Assembly District is unique citywide
        (
            SELECT ap.borocode
            FROM {{ ref('stg__atomicpolygons') }} AS ap
            WHERE
                ap.electdist = ed.electdist
                AND ap.assemdist = ed.assembly_district
            ORDER BY ap.atomicid
            LIMIT 1
        ) AS borough
    FROM {{ ref('stg__electiondistrict') }} AS ed
)

SELECT
    globalid,
    election_district,
    assembly_district,
    congress_district,
    state_sen_district,
    muni_court_district,
    city_council_district,
    borough
FROM election_districts_with_borough
WHERE borough IS NOT NULL  -- Only include election districts with a valid borough
ORDER BY assembly_district, election_district
