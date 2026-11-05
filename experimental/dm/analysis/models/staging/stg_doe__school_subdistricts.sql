SELECT
    district::INTEGER AS school_district,
    subdistrict,
    name AS subdistrict_name,
    geom
FROM {{ source('sources', 'doe_school_subdistricts') }}
