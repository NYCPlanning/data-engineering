SELECT
    school_district || '-' || subdistrict AS school_subdistrict_id,
    community_districts AS ahft_community_districts,
    ahft_priority,
    barely_intersects_cd AS ahft_barely_intersects_cd,
    ps_students AS ps_students_2036,
    ps_seats AS ps_seats_2036,
    ps_utilization AS ps_utilization_2036,
    is_students AS is_students_2036,
    is_seats AS is_seats_2036,
    is_utilization AS is_utilization_2036
FROM {{ ref('stg_ahft__school_subdistricts') }}
