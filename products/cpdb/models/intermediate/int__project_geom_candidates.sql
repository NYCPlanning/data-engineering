-- every geometry found for a project in the Capital Commitment Plan, ranked. A project's
-- geometry is its top-ranked candidate (see int__project_geom).
WITH priorities (candidate, method, priority) AS (
    VALUES
    ('dcp_cpdb_agencyverified', 'agency verified', 1),
    ('ddc_capitalprojects_publicbuildings', 'agency dataset', 2),
    ('ddc_capitalprojects_infrastructure', 'agency dataset', 3),
    ('edc_capitalprojects_ferry', 'agency dataset', 4),
    ('edc_capitalprojects', 'agency dataset', 5),
    ('dpr_capitalprojects', 'agency dataset', 6),
    ('dot_projects_streets', 'agency dataset', 7),
    ('dot_projects_intersections', 'agency dataset', 8),
    ('dot_projects_bridges', 'agency dataset', 9),
    ('dcp_manual_geoms_2020', 'dcp curated', 10),
    ('dcp_id_bin_map', 'dcp curated', 11),
    ('dcp_json', 'dcp curated', 12),
    ('sprints', 'dcp curated', 13),
    ('dpr_park_id', 'string match', 14),
    ('dpr_park_name_round1', 'string match', 15),
    ('dpr_park_name_round2', 'string match', 16),
    ('dpr_park_name_round3', 'string match', 17),
    ('facility_name', 'string match', 18),
    ('library_name', 'string match', 19)
),

candidates AS (
    SELECT * FROM {{ ref('int__project_geom_candidates_agency_verified') }}
    UNION ALL
    SELECT * FROM {{ ref('int__project_geom_candidates_agency_datasets') }}
    UNION ALL
    SELECT * FROM {{ ref('int__project_geom_candidates_dcp_curated') }}
    UNION ALL
    SELECT * FROM {{ ref('int__project_geom_candidates_string_match') }}
)

SELECT
    c.maprojid,
    c.candidate,
    p.method,
    p.priority,
    c.geomsource,
    c.datasource,
    c.dataname,
    c.geom
FROM candidates AS c
INNER JOIN priorities AS p ON c.candidate = p.candidate
WHERE
    c.maprojid IN (SELECT maprojid FROM {{ ref('int__ccp_projects') }})
    -- a match can land on a source row without a geometry, which leaves the project to the
    -- next candidate
    AND c.geom IS NOT NULL
