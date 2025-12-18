{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['b7sc']},
    ]
) }}

SELECT
    b7sc,
    lookup_key,
    place_name_sort_order,
    saf_place_name,
    face_code,
    feature_type
FROM {{ ref('stg__facecode_and_featurename') }}
WHERE principal_flag = 'Y'
