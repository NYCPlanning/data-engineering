SELECT
    school_district || '-' || subdistrict AS school_subdistrict_id,
    school_district,
    subdistrict,
    subdistrict_name,
    geom
FROM {{ ref('stg_doe__school_subdistricts') }}
