{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']}
    ]
) }}
SELECT
    boro.boroughcode,
    nsf.segmentid,
    seqnum.segment_seqnum,
    CASE
        -- Non-physical census block boundary 
        WHEN nsf.linetype = 3 THEN '3'
        -- Non-physical boundary other than census 
        WHEN nsf.linetype IN (1, 2, 6) THEN '7'
        -- Physical boundary such as cemetery wall 
        WHEN nsf.linetype IN (4, 5) THEN '8'
        -- Other non-street feature
        WHEN nsf.linetype = 7 THEN '4'
    END AS feature_type_code,
    'U' AS segment_type,
    '*' AS from_level_code,
    '*' AS to_level_code,
    nsf.legacy_segmentid,
    nsf.geom,
    nsf.shape_length,
    'nonstreetfeatures' AS source_feature_type,
    TRUE AS include_in_geosupport_lion,
    TRUE AS include_in_bytes_lion
FROM {{ source("recipe_sources", "dcp_cscl_nonstreetfeatures") }} AS nsf
LEFT JOIN {{ ref("int__streetcode_and_facecode") }} AS boro ON nsf.segmentid = boro.segmentid
LEFT JOIN {{ ref("int__nonstreetfeature_seqnum") }} AS seqnum ON nsf.segmentid = seqnum.segmentid
