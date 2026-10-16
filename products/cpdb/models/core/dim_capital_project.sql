WITH lifecycle_totals AS (
    SELECT
        maprojid,
        lifecycle,
        SUM(plannedcommit_total) AS plannedcommit_total
    FROM {{ ref('fact_planned_commitment') }}
    WHERE lifecycle IS NOT NULL
    GROUP BY maprojid, lifecycle
),

lifecycles AS (
    SELECT
        maprojid,
        lifecycle,
        plannedcommit_total,
        COUNT(*) OVER (PARTITION BY maprojid) AS lifecycles,
        -- ties go to the lifecycle that sorts first, so the pick is stable
        ROW_NUMBER() OVER (PARTITION BY maprojid ORDER BY plannedcommit_total DESC, lifecycle ASC) AS rk
    FROM lifecycle_totals
),

commitment_totals AS (
    SELECT
        f.maprojid,
        r.name AS commitment_description,
        SUM(f.plannedcommit_total) AS plannedcommit_total
    FROM {{ ref('fact_planned_commitment') }} AS f
    INNER JOIN {{ ref('ref_commitment_code') }} AS r ON f.commitment_code = r.commitment_code
    GROUP BY f.maprojid, r.name
),

commitments AS (
    SELECT
        maprojid,
        commitment_description,
        plannedcommit_total,
        COUNT(*) OVER (PARTITION BY maprojid) AS commitment_descriptions,
        ROW_NUMBER() OVER (
            PARTITION BY maprojid ORDER BY plannedcommit_total DESC, commitment_description ASC
        ) AS rk
    FROM commitment_totals
)

SELECT
    p.ccpversion AS ccp_version,
    p.maprojid,
    p.magency,
    p.magencyacro,
    p.projectid,
    p.description,
    p.plannedcommit_total,
    l.lifecycle,
    l.plannedcommit_total / NULLIF(p.plannedcommit_total, 0) AS lifecycle_share,
    COALESCE(l.lifecycles > 1, FALSE) AS has_multiple_lifecycles,
    c.commitment_description,
    c.plannedcommit_total / NULLIF(p.plannedcommit_total, 0) AS commitment_description_share,
    COALESCE(c.commitment_descriptions > 1, FALSE) AS has_multiple_commitment_descriptions
FROM {{ ref('int__ccp_projects') }} AS p
LEFT JOIN lifecycles AS l ON p.maprojid = l.maprojid AND l.rk = 1
LEFT JOIN commitments AS c ON p.maprojid = c.maprojid AND c.rk = 1
