SELECT
    ccpversion,
    maprojid,
    magency,
    magencyacro,
    projectid,
    description,
    NULL::text AS typecategory,
    NULL::text AS geomsource,
    NULL::text AS dataname,
    NULL::text AS datasource,
    NULL::timestamp AS datadate,
    NULL::geometry AS geom
FROM {{ ref('int__ccp_projects') }}
