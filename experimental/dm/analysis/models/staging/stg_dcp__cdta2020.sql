SELECT
    cdta2020,
    cdtaname AS cdta_name,
    cdtatype AS cdta_type,
    borocode,
    boroname AS boro_name
FROM {{ source('sources', 'dcp_cdta2020') }}
