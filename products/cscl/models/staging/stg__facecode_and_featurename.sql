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
        b10sc
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
remove_dash_slash_spaces AS (
    SELECT
        regexp_replace(lookup_key, '\s*(/|-)\s*', '\1', 'g') AS lookup_key,
        globalid
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
            unnest(regexp_matches(lookup_key, '(?:[^\s]*[\-\/][^\s]*)|[\d]+|[^\d\s]+', 'g')) AS match,
            globalid
        FROM remove_dash_slash_spaces
    ) AS t
    GROUP BY globalid
)
SELECT
    b10sc,
    b7sc,
    lookup_key,
    sort_order.place_name_sort_order,
    face_code,
    feature_type,
    primary_flag,
    principal_flag,
    enders_flag,
    exception_flag,
    snd_feature_type,
    horizontal_topo_type,
    globalid
FROM all_features
LEFT JOIN sort_order ON all_features.globalid = sort_order.globalid
