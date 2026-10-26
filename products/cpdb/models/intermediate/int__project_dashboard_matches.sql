-- the dashboard's FMS ID has no agency code, so a project matches on FMS ID and managing
-- agency. Both sources name the agency from the same three-digit code, sometimes with
-- different acronyms (see dashboard_managing_agencies), so a project ID with one project on
-- each side also matches when the acronyms differ
WITH dashboard_projects AS (
    SELECT
        fms_id,
        managing_agency,
        COUNT(*) OVER (PARTITION BY fms_id) AS agencies
    FROM {{ ref('int__dashboard_agency_projects') }}
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
)

SELECT
    f.maprojid,
    d.fms_id,
    d.managing_agency,
    f.managing_agency = d.managing_agency AS same_agency
FROM fisa_projects AS f
INNER JOIN dashboard_projects AS d
    ON
        f.fms_id = d.fms_id
        AND (f.managing_agency = d.managing_agency OR (f.candidates = 1 AND d.agencies = 1))
