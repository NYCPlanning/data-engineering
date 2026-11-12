SELECT * FROM {{ ref('int_cd_cpdb_project_spine') }}
ORDER BY borocd, maprojid
