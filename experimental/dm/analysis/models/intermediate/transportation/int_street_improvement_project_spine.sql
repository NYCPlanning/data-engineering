SELECT
    project_name || ' (' || sip_year || ')' AS street_improvement_project_id,
    project_name,
    'Corridor' AS project_type,
    sip_year,
    end_date,
    geom
FROM {{ ref('stg_dot__vzv_sip_corridors') }}
UNION ALL
SELECT
    project_name || ' (' || sip_year || ')' AS street_improvement_project_id,
    project_name,
    'Intersection' AS project_type,
    sip_year,
    end_date,
    geom
FROM {{ ref('stg_dot__vzv_sip_intersections') }}
