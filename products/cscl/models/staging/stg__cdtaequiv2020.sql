SELECT
    cdta_code,
    cdta_name,
    cdta_type

FROM {{ source("recipe_sources", "dcp_cscl_cdtaequiv2020") }}
