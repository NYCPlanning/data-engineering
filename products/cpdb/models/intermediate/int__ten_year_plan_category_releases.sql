SELECT
    n.budget_proj_type AS project_type,
    n.typc,
    n.typc_name AS name,
    r.fisa_version,
    r.release_order
FROM {{ ref('fisa_typc_names') }} AS n
INNER JOIN {{ ref('int__fisa_releases') }} AS r ON n.version = r.fisa_version
