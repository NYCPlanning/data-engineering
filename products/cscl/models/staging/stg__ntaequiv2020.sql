SELECT
    nta_code,
    nta_name,
    nta_abbrev,
    nta_type

FROM {{ source("recipe_sources", "dcp_cscl_ntaequiv2020") }}
