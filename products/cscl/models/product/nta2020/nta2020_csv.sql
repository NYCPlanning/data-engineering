SELECT
    nta_code AS "NTA_CODE",
    nta_name AS "NTA_NAME",
    nta_abbrev AS "NTA_ABBREV",
    nta_type AS "NTA_TYPE"
FROM {{ ref('nta2020_by_field_csv') }}
ORDER BY nta_code
