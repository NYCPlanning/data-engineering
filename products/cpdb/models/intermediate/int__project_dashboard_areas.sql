-- the boroughs and community districts the Capital Projects Dashboard gives each project, in
-- its latest reporting period
WITH dashboard AS (
    SELECT
        fms_id,
        managing_agency,
        pid,
        community_board
    FROM {{ ref('stg__moo_capital_projects_budget_schedule') }}
    WHERE
        reporting_period
        = (SELECT MAX(reporting_period) FROM {{ ref('stg__moo_capital_projects_budget_schedule') }})
),

-- community_board can list several areas, like "Queens, Queens 07": a community district, a
-- bare borough, or Citywide
areas AS (
    SELECT
        d.fms_id,
        d.managing_agency,
        d.pid,
        TRIM(area) AS area
    FROM dashboard AS d
    CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(d.community_board, ',') AS area
),

parsed AS (
    SELECT
        fms_id,
        managing_agency,
        pid,
        area = 'Citywide' AS is_citywide,
        SUBSTRING(area FROM '^(Manhattan|Bronx|Brooklyn|Queens|Staten Island)') AS borough,
        CASE
            WHEN area ~ '^(Manhattan|Bronx|Brooklyn|Queens|Staten Island) \d{2}$'
                THEN
                    CASE SUBSTRING(area FROM '^(.*) \d{2}$')
                        WHEN 'Manhattan' THEN '1'
                        WHEN 'Bronx' THEN '2'
                        WHEN 'Brooklyn' THEN '3'
                        WHEN 'Queens' THEN '4'
                        WHEN 'Staten Island' THEN '5'
                    END || SUBSTRING(area FROM '\d{2}$')
        END AS community_district
    FROM areas
),

-- one row per dashboard agency project, so a project's rows can be checked for a missing
-- community district
dashboard_rows AS (
    SELECT
        fms_id,
        managing_agency,
        pid,
        BOOL_OR(is_citywide) AS is_citywide,
        COUNT(community_district) > 0 AS has_community_district
    FROM parsed
    GROUP BY fms_id, managing_agency, pid
),

dashboard_projects AS (
    SELECT
        fms_id,
        managing_agency,
        COUNT(*) OVER (PARTITION BY fms_id) AS agencies
    FROM dashboard_rows
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
matches AS (
    SELECT
        f.maprojid,
        d.fms_id,
        d.managing_agency
    FROM fisa_projects AS f
    INNER JOIN dashboard_projects AS d
        ON
            f.fms_id = d.fms_id
            AND (f.managing_agency = d.managing_agency OR (f.candidates = 1 AND d.agencies = 1))
),

row_summary AS (
    SELECT
        m.maprojid,
        COUNT(*) AS dashboard_rows,
        BOOL_AND(r.has_community_district) AS every_row_has_community_district,
        BOOL_OR(r.is_citywide) AS is_citywide
    FROM matches AS m
    INNER JOIN dashboard_rows AS r
        ON m.fms_id = r.fms_id AND m.managing_agency = r.managing_agency
    GROUP BY m.maprojid
),

area_summary AS (
    SELECT
        m.maprojid,
        ARRAY_AGG(DISTINCT p.borough) FILTER (WHERE p.borough IS NOT NULL) AS boroughs,
        ARRAY_AGG(DISTINCT p.community_district)
        FILTER (WHERE p.community_district IS NOT NULL) AS community_districts
    FROM matches AS m
    INNER JOIN parsed AS p
        ON m.fms_id = p.fms_id AND m.managing_agency = p.managing_agency
    GROUP BY m.maprojid
)

SELECT
    r.maprojid,
    r.dashboard_rows,
    r.every_row_has_community_district,
    r.is_citywide,
    a.boroughs,
    a.community_districts
FROM row_summary AS r
INNER JOIN area_summary AS a ON r.maprojid = a.maprojid
