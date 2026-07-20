WITH all_records AS (
    SELECT * FROM {{ ref('int__maprojid_cd') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_censustracts') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_congressionaldistricts') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_councildistricts') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_firecompanies') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_municipalcourtdistricts') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_policeprecincts') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_schooldistricts') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_stateassemblydistricts') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_statesenatedistricts') }}
    UNION ALL
    SELECT * FROM {{ ref('int__maprojid_trafficanalysiszones') }}
)

SELECT DISTINCT
    feature_id,
    admin_boundary_type,
    admin_boundary_id
FROM all_records
ORDER BY feature_id, admin_boundary_type, admin_boundary_id
