-- ThinLION lookups that find no feature, one row per distinct lookup with the number of
-- atomic polygons it leaves blank (record_count). Prod's ThinLION extract does the same
-- three lookups per atomic polygon and logs each miss once per atomic polygon: "Could not
-- find a {feature class} feature using query '{query}'." The message repeats prod's wording
-- and query, built from our join keys.
WITH thinlion AS (
    SELECT * FROM {{ ref('thinlion_by_field_unformatted') }}
),
censustract2010 AS (
    SELECT * FROM {{ ref('stg__censustract2010') }}
),
misses AS (
    SELECT
        'CensusTract2020' AS feature_class,
        'CSCL-LOG-THIN-01' AS log_message_id,
        FORMAT('BOROCT=''{}{}''', borough, censustract_2020) AS query
    FROM thinlion
    WHERE ct2020_globalid IS NULL
    UNION ALL
    SELECT
        'CensusTract2010' AS feature_class,
        'CSCL-LOG-THIN-02' AS log_message_id,
        FORMAT('BOROCT=''{}{}''', borough, censustract_2010) AS query
    FROM thinlion
    WHERE ct2010_globalid IS NULL
    UNION ALL
    SELECT
        'HealthArea' AS feature_class,
        'CSCL-LOG-THIN-03' AS log_message_id,
        FORMAT(
            'HEALTHAREA=''{}'' AND BOROUGH = ''{}''',
            COALESCE(censustract2010.health_area, ''),
            thinlion.borough
        ) AS query
    FROM thinlion
    LEFT JOIN censustract2010 ON thinlion.ct2010_globalid = censustract2010.globalid
    WHERE thinlion.healtharea_globalid IS NULL
)
SELECT
    'warning' AS log_level,
    'thinlion lookup not found' AS error_category,
    log_message_id,
    'atomicpolygons' AS source_feature_layer,
    {{ log_message("Could not find a {} feature using query '{}'.", ["feature_class", "query"]) }} AS message,
    COUNT(*) AS record_count
FROM misses
GROUP BY feature_class, log_message_id, query
