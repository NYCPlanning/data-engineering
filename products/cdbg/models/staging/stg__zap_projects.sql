SELECT
    "Project_Id" AS project_id,
    "ALL_ZAP_BBLs" AS bbls
FROM {{ source("recipe_sources", "dcp_projectbbls_cy") }}
WHERE "ALL_ZAP_BBLs" IS NOT NULL
