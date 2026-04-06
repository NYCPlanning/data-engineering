SELECT
    cdta_code AS "CDTA_CODE",
    cdta_name AS "CDTA_NAME",
    cdta_type AS "CDTA_TYPE"
FROM {{ ref('cdta2020_by_field_csv') }}
ORDER BY cdta_code
