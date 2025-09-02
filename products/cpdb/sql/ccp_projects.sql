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
    a.maprojid,
    b.description,
    plannedcommit_ccnonexempt,
    plannedcommit_ccexempt,
    plannedcommit_citycost,
    plannedcommit_nccstate,
    plannedcommit_nccfederal,
    plannedcommit_nccother,
    plannedcommit_noncitycost,
    plannedcommit_total,
    CASE
        WHEN UPPER(b.description) LIKE '%BNY%' AND a.magency = '801' THEN 'BNY'
        WHEN LOWER(b.description) LIKE '%governor%s island%' AND a.magency = '801' THEN 'TGI'
        ELSE c.cape_agencyacronym
    END AS magencyacro,
    CASE
        WHEN UPPER(b.description) LIKE '%BNY%' AND a.magency = '801' THEN 'Brooklyn Navy Yard'
        WHEN LOWER(b.description) LIKE '%governor%s island%' AND a.magency = '801' THEN 'Trust for Governors Island'
        ELSE c.cape_agency
    END AS magencyname
FROM projects AS a
INNER JOIN descriptions AS b ON a.maprojid = b.maprojid AND b.rk = 1
INNER JOIN dcp_agencylookup AS c ON a.magency = LPAD(c.cape_agencycode, 3, '0');
