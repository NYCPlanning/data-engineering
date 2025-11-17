{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    *,
    CASE
        -- With
        WHEN trafdir = 'FT' THEN 'W'
        -- Against
        WHEN trafdir = 'TF' THEN 'A'
        -- Non-vehicular
        WHEN trafdir = 'NV' THEN 'P'
        -- Two-way
        WHEN trafdir = 'TW' THEN 'T'
    END AS traffic_direction,
    CASE
        -- Paper street that is not also a boundary 
        WHEN status = '3' THEN '5'
        -- Private street that exists physically 
        WHEN status = '2' AND rwjurisdiction = '3' THEN '6'
        -- Paper street that coincides with a non-physical boundary 
        WHEN status = '9' THEN '9'
        -- Alley
        WHEN rw_type = 10 THEN 'A'
        -- Path, non-vehicular, addressable
        WHEN
            trafdir = 'NV'
            AND (
                l_low_hn != '0'
                OR l_high_hn != '0'
                OR r_low_hn != '0'
                OR r_high_hn != '0'
            )
            THEN 'W'
        -- Ferry
        WHEN rw_type = 14 THEN 'F'
        -- Constructed
        WHEN status = '2' AND rwjurisdiction = '5' THEN 'C'
        -- Public street, bridge or tunnel that exists physically (or its generic geometry), other than Feature Type Code W  -- redundant but to be explicit about above case
    END AS feature_type_code,
    'centerline' AS feature_type,
    'centerline' AS source_table,
    (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AND rw_type != 8 AS include_in_geosupport_lion,
    (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AS include_in_bytes_lion
FROM {{ source("recipe_sources", "dcp_cscl_centerline") }}
