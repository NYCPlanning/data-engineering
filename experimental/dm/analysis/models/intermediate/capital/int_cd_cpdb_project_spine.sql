-- one row per district and project, summing CPDB's district shares across budget line types
SELECT
    borocd,
    maprojid,
    sum(planned_commitments_in_district) AS planned_commitments_in_district,
    any_value(geometry_source) AS geometry_source
FROM {{ ref('stg_cpdb__projects_by_community_district') }}
GROUP BY borocd, maprojid
