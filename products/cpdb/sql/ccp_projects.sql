DROP TABLE IF EXISTS ccp_projects;
CREATE TABLE ccp_projects AS
WITH projects AS (
    SELECT
        ccpversion,
        magency,
        projectid,
        maprojid,
        SUM(plannedcommit_ccnonexempt) AS plannedcommit_ccnonexempt,
        SUM(plannedcommit_ccexempt) AS plannedcommit_ccexempt,
        SUM(plannedcommit_citycost) AS plannedcommit_citycost,
        SUM(plannedcommit_nccstate) AS plannedcommit_nccstate,
        SUM(plannedcommit_nccfederal) AS plannedcommit_nccfederal,
        SUM(plannedcommit_nccother) AS plannedcommit_nccother,
        SUM(plannedcommit_noncitycost) AS plannedcommit_noncitycost,
        SUM(plannedcommit_total) AS plannedcommit_total
    FROM ccp_commitments
    GROUP BY maprojid, magency, projectid, ccpversion
),

descriptions AS (
    SELECT
        maprojid,
        projectdescription AS description,
        ROW_NUMBER() OVER (
            PARTITION BY maprojid
            ORDER BY LENGTH(REPLACE(projectdescription, ' ', '')) DESC
        ) AS rk
    FROM ccp_commitments
)

SELECT
    ccpversion,
    magency,
    projectid,
    projects.maprojid,
    descriptions.description,
    plannedcommit_ccnonexempt,
    plannedcommit_ccexempt,
    plannedcommit_citycost,
    plannedcommit_nccstate,
    plannedcommit_nccfederal,
    plannedcommit_nccother,
    plannedcommit_noncitycost,
    plannedcommit_total,
    CASE
        WHEN UPPER(descriptions.description) LIKE '%BNY%' AND projects.magency = '801' THEN 'BNY'
        WHEN LOWER(descriptions.description) LIKE '%governor%s island%' AND projects.magency = '801' THEN 'TGI'
        ELSE dcp_managing_agencies_lookup.managing_agency_acronym
    END AS magencyacro,
    CASE
        WHEN UPPER(descriptions.description) LIKE '%BNY%' AND projects.magency = '801' THEN 'Brooklyn Navy Yard'
        WHEN
            LOWER(descriptions.description) LIKE '%governor%s island%' AND projects.magency = '801'
            THEN 'Trust for Governors Island'
        ELSE dcp_managing_agencies_lookup.managing_agency
    END AS magencyname
FROM projects
INNER JOIN descriptions ON projects.maprojid = descriptions.maprojid AND descriptions.rk = 1
INNER JOIN dcp_managing_agencies_lookup ON projects.magency = dcp_managing_agencies_lookup.agency_code;
