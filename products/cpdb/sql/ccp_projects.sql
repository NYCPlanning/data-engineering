DROP TABLE IF EXISTS ccp_projects;
CREATE TABLE ccp_projects AS
WITH projects AS (
    SELECT
        ccpversion,
        magency,
        projectid,
        maprojid,
        sum(plannedcommit_ccnonexempt) AS plannedcommit_ccnonexempt,
        sum(plannedcommit_ccexempt) AS plannedcommit_ccexempt,
        sum(plannedcommit_citycost) AS plannedcommit_citycost,
        sum(plannedcommit_nccstate) AS plannedcommit_nccstate,
        sum(plannedcommit_nccfederal) AS plannedcommit_nccfederal,
        sum(plannedcommit_nccother) AS plannedcommit_nccother,
        sum(plannedcommit_noncitycost) AS plannedcommit_noncitycost,
        sum(plannedcommit_total) AS plannedcommit_total
    FROM ccp_commitments
    GROUP BY maprojid, magency, projectid, ccpversion
),
descriptions AS (
    SELECT
        maprojid,
        projectdescription AS description,
        row_number()
            OVER
            (
                PARTITION BY maprojid
                ORDER BY length(replace(projectdescription, ' ', '')) DESC
            )
        AS rk
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
        WHEN upper(b.description) LIKE '%BNY%' AND a.magency = '801' THEN 'BNY'
        WHEN lower(b.description) LIKE '%governor%s island%' AND a.magency = '801' THEN 'TGI'
        ELSE c.cape_agencyacronym
    END AS magencyacro,
    CASE
        WHEN upper(b.description) LIKE '%BNY%' AND a.magency = '801' THEN 'Brooklyn Navy Yard'
        WHEN lower(b.description) LIKE '%governor%s island%' AND a.magency = '801' THEN 'Trust for Governors Island'
        ELSE c.cape_agency
    END AS magencyname
FROM projects AS a
INNER JOIN descriptions AS b ON a.maprojid = b.maprojid AND b.rk = 1
INNER JOIN dcp_agencylookup AS c ON a.magency = lpad(c.cape_agencycode, 3, '0');
