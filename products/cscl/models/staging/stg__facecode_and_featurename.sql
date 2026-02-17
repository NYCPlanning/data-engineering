{{ config(materialized='table') }}

WITH all_features AS (
    SELECT
        b10sc,
        b7sc,
        lookup_key, -- TODO as feature_name
        facecode AS face_code,
        'street' AS feature_type,
        primary_flag,
        principal_flag,
        enders_flag,
        exception_flag,
        snd_feature_type,
        horizontal_topo_type,
        globalid
    FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
    UNION ALL
    SELECT
        b10sc,
        b7sc,
        lookup_key,
        facecode AS face_code,
        'feature' AS feature_type,
        primary_flag,
        principal_flag,
        enders_flag,
        exception_flag,
        snd_feature_type,
        NULL AS horizontal_topo_type,
        globalid
    FROM {{ source("recipe_sources", "dcp_cscl_featurename") }}
),
trimmed_key AS (
    SELECT
        regexp_replace(lookup_key, '\s*(/|-)\s*', '\1', 'g') AS lookup_key_trimmed,
        *
    FROM all_features
),
sort_order AS (
    SELECT
        string_agg(
            CASE
                WHEN match ~ '^\d+$' THEN lpad(match, 4, ' ')
                ELSE match
            END,
            ' '
        ) AS place_name_sort_order,
        globalid
    FROM (
        SELECT
            unnest(
                -- NOTE - this regex is tested in _stg.yml with unit test 'test__stg__facecode_and_featurename_sortname'
                regexp_matches(
                    lookup_key_trimmed,
                    '(?:(?:[^\d\-\/\s]+|\d+)?[\-\/])+(?:[^\d\-\/\s]+|\d+)?|\d+|[^\d\s]+',
                    'g'
                )
            ) AS match,
            globalid
        FROM trimmed_key
    ) AS t
    GROUP BY globalid
)
SELECT
    left(b10sc, 1) AS boroughcode,
    b10sc,
    b7sc,
    lookup_key,
    lookup_key_trimmed,
    sort_order.place_name_sort_order,
    face_code,
    feature_type,
    primary_flag,
    principal_flag,
    enders_flag,
    exception_flag,
    snd_feature_type,
    horizontal_topo_type,
    trimmed_key.globalid
FROM trimmed_key
LEFT JOIN sort_order ON trimmed_key.globalid = sort_order.globalid
