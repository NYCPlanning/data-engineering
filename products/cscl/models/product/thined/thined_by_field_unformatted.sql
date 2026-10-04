-- Borough of each ED (Election District + Assembly District is unique citywide): the
-- borough of its lowest-atomicid AtomicPolygon. Computed in one pass rather than as a
-- per-ED correlated subquery, which scanned every AP once per ED (~10+ min).
WITH ed_borough AS (
    SELECT DISTINCT ON (electdist, assemdist)
        electdist,
        assemdist,
        borocode
    FROM {{ ref('stg__atomicpolygons') }}
    ORDER BY electdist, assemdist, atomicid
),

election_districts_with_borough AS (
    SELECT
        ed.globalid,
        ed.electdist AS election_district,
        ed.assembly_district,
        ed.congress_district,
        ed.state_sen_district,
        ed.muni_court_district,
        ed.city_council_district,
        eb.borocode AS borough
    FROM {{ ref('stg__electiondistrict') }} AS ed
    LEFT JOIN ed_borough AS eb
        ON ed.electdist = eb.electdist AND ed.assembly_district = eb.assemdist
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
