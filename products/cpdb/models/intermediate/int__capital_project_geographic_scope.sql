-- each project's mappability and geographic scope, from the first signal that has one:
-- CPDB's geometry, the agency-verified label, the Capital Projects Dashboard's community
-- board, then the project's ten-year plan categories
WITH geometries AS (
    SELECT
        cleaned.maprojid,
        -- CPDB buffers lines into polygons, so the type comes from the geometry before that
        CASE
            WHEN ST_GEOMETRYTYPE(raw.geom) IN ('ST_LineString', 'ST_MultiLineString') THEN 'network'
            WHEN ST_NUMGEOMETRIES(cleaned.geom) > 1 THEN 'multiple sites'
            ELSE 'site'
        END AS geographic_scope
    FROM {{ ref('cpdb_dcpattributes') }} AS cleaned
    INNER JOIN {{ ref('int__project_geom') }} AS raw
        ON cleaned.maprojid = raw.maprojid
    WHERE cleaned.geom IS NOT NULL
),

agency_labels AS (
    SELECT DISTINCT ON (a.maprojid)
        a.maprojid,
        m.mappability,
        m.geographic_scope
    FROM {{ ref('dcp_cpdb_agencyverified') }} AS a
    INNER JOIN {{ ref('agency_verified_mappability') }} AS m ON a.mappable = m.mappable
    ORDER BY a.maprojid ASC, a.mappable ASC
),

dashboard_areas AS (
    SELECT
        fms_id,
        managing_agency,
        CASE
            WHEN community_board ~ '^(Manhattan|Bronx|Brooklyn|Queens|Staten Island) \d{2}$'
                THEN community_board
        END AS community_district,
        CASE
            WHEN community_board IN ('Manhattan', 'Bronx', 'Brooklyn', 'Queens', 'Staten Island')
                THEN community_board
            WHEN community_board ~ '^(Manhattan|Bronx|Brooklyn|Queens|Staten Island) \d{2}$'
                THEN REGEXP_REPLACE(community_board, ' \d{2}$', '')
        END AS borough
    FROM {{ ref('int__dashboard_agency_projects') }}
),

-- a dashboard project is an FMS ID and managing agency, and its agency projects can be in
-- different places: it's one community district only when every row names the same one,
-- and one borough when every row is in the same borough
dashboard_scopes AS (
    SELECT
        fms_id,
        managing_agency,
        COUNT(*) OVER (PARTITION BY fms_id) AS agencies,
        CASE
            WHEN COUNT(*) = COUNT(community_district) AND COUNT(DISTINCT community_district) = 1
                THEN 'community district'
            WHEN COUNT(*) = COUNT(borough) AND COUNT(DISTINCT borough) = 1
                THEN 'borough'
        END AS geographic_scope
    FROM dashboard_areas
    GROUP BY fms_id, managing_agency
),

fisa_projects AS (
    SELECT
        projects.maprojid,
        COALESCE(agencies.dashboard_managing_agency, projects.magencyacro) AS managing_agency,
        TRIM(projects.projectid) AS fms_id,
        COUNT(*) OVER (PARTITION BY TRIM(projects.projectid)) AS candidates
    FROM {{ ref('int__ccp_projects') }} AS projects
    LEFT JOIN {{ ref('dashboard_managing_agencies') }} AS agencies
        ON projects.magency = agencies.magency::text
),

-- the dashboard's FMS ID has no agency code, so a project matches on FMS ID and managing
-- agency. Both sources name the agency from the same three-digit code, sometimes with
-- different acronyms (see dashboard_managing_agencies), so a project ID with one project on
-- each side also matches when the acronyms differ
dashboard_matches AS (
    SELECT
        f.maprojid,
        d.geographic_scope
    FROM fisa_projects AS f
    INNER JOIN dashboard_scopes AS d
        ON
            f.fms_id = d.fms_id
            AND (f.managing_agency = d.managing_agency OR (f.candidates = 1 AND d.agencies = 1))
),

place_link_totals AS (
    SELECT
        f.maprojid,
        d.place_link,
        SUM(f.plannedcommit_total) AS plannedcommit_total
    FROM {{ ref('fact_planned_commitment') }} AS f
    INNER JOIN {{ ref('dim_ten_year_plan_category') }} AS d
        ON f.project_type = d.project_type AND f.typc = d.typc
    WHERE d.place_link IS NOT NULL
    GROUP BY f.maprojid, d.place_link
),

place_links AS (
    SELECT DISTINCT ON (maprojid)
        maprojid,
        place_link
    FROM place_link_totals
    ORDER BY maprojid ASC, plannedcommit_total DESC, place_link ASC
)

SELECT
    p.maprojid,
    CASE
        WHEN g.maprojid IS NOT NULL THEN 'mapped'
        WHEN a.mappability IS NOT NULL THEN a.mappability
        WHEN d.geographic_scope IS NOT NULL THEN 'place-specific but unmapped'
        WHEN l.place_link IN ('site', 'network') THEN 'place-specific but unmapped'
        WHEN l.place_link IN ('fleet', 'system') THEN 'not place-specific'
    END AS mappability,
    CASE
        WHEN g.maprojid IS NOT NULL THEN g.geographic_scope
        WHEN a.mappability IS NOT NULL THEN a.geographic_scope
        WHEN d.geographic_scope IS NOT NULL THEN d.geographic_scope
        WHEN l.place_link = 'network' THEN 'network'
        WHEN l.place_link IN ('fleet', 'system') THEN 'citywide'
    END AS geographic_scope,
    CASE
        WHEN g.maprojid IS NOT NULL THEN 'geometry'
        WHEN a.mappability IS NOT NULL THEN 'agency verified'
        WHEN d.geographic_scope IS NOT NULL THEN 'dashboard'
        WHEN l.place_link IS NOT NULL THEN 'category'
    END AS scope_source
FROM {{ ref('int__ccp_projects') }} AS p
LEFT JOIN geometries AS g ON p.maprojid = g.maprojid
LEFT JOIN agency_labels AS a ON p.maprojid = a.maprojid
LEFT JOIN dashboard_matches AS d ON p.maprojid = d.maprojid
LEFT JOIN place_links AS l ON p.maprojid = l.maprojid
