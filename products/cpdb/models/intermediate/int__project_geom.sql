-- one row per project: its top-ranked geometry candidate, the other candidates, and why the
-- geometry is dropped from the product, if it is
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY maprojid ORDER BY priority) AS rk
    FROM {{ ref('int__project_geom_candidates') }}
),

candidate_lists AS (
    SELECT
        maprojid,
        COUNT(*) AS candidate_count,
        STRING_AGG(candidate, ', ' ORDER BY priority) AS candidates,
        STRING_AGG(DISTINCT method, ', ') AS candidate_methods
    FROM ranked
    GROUP BY maprojid
),

agency_mappability AS (
    SELECT
        maprojid,
        BOOL_OR(mappable = 'No - Can never be mapped') AS never_mappable,
        BOOL_OR(mappable = 'No - Can be in future') AS maybe_mappable
    FROM {{ ref('dcp_cpdb_agencyverified') }}
    GROUP BY maprojid
),

selected AS (
    SELECT
        p.maprojid,
        r.candidate,
        r.method,
        r.geomsource,
        r.datasource,
        r.dataname,
        ST_MAKEVALID(r.geom) AS geom
    FROM {{ ref('int__ccp_projects') }} AS p
    LEFT JOIN ranked AS r ON p.maprojid = r.maprojid AND r.rk = 1
)

SELECT
    s.maprojid,
    s.candidate,
    s.method,
    s.geomsource,
    s.datasource,
    s.dataname,
    COALESCE(l.candidate_count, 0) AS candidate_count,
    l.candidates,
    l.candidate_methods,
    -- geometries from the agency datasets are trusted over DCP's flags. `geomsource NOT IN`
    -- is null for a null geomsource, so those geometries are kept.
    CASE
        WHEN s.geom IS NULL THEN NULL
        WHEN
            s.maprojid IN (SELECT maprojid FROM {{ ref('cpdb_badgeoms') }})
            AND s.geomsource NOT IN ('dpr', 'dot', 'ddc')
            THEN 'flagged bad geometry'
        WHEN a.never_mappable THEN 'agency: can never be mapped'
        WHEN a.maybe_mappable AND s.geomsource NOT IN ('dpr', 'dot', 'ddc', 'edc')
            THEN 'agency: can be mapped in future'
    END AS geom_removed_reason,
    s.geom
FROM selected AS s
LEFT JOIN candidate_lists AS l ON s.maprojid = l.maprojid
LEFT JOIN agency_mappability AS a ON s.maprojid = a.maprojid
