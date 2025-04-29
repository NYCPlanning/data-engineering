SELECT *
FROM {{ source("recipe_sources", "dcp_cscl_altsegmentdata") }}
WHERE alt_segdata_type <> 'S' -- S denotes a SAF record
