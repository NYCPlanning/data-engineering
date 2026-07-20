WITH base AS (
    SELECT * FROM {{ ref('int__dcpattributes_geom_agencyverified') }}
),

-- gap-fill: DPR park id embedded in the project description (e.g. "Q123")
dpr_string_id AS (
    SELECT DISTINCT ON (base.maprojid)
        base.maprojid,
        b.wkb_geometry AS geom,
        'dpr'::text AS geomsource,
        'dpr'::text AS datasource,
        'dpr_parksproperties'::text AS dataname
    FROM base
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON SUBSTRING(base.description FROM '[BMQRX][0-9][0-9][0-9]') = b.gispropnum
    WHERE base.magencyacro = 'DPR' AND base.geom IS NULL
    ORDER BY base.maprojid ASC, b.gispropnum ASC
),

-- gap-fill: DPR park name fuzzy-matched against the project description, 3 rounds --
-- round 1 excludes generic signnames incl. 'BRIDGE PARK', round 2 allows 'BRIDGE PARK' back in,
-- round 3 re-excludes it but matches via levenshtein distead of LIKE. All 3 rounds are pure
-- gap-fill (never overwrite), so priority-coalescing 3 independently-computed rounds against the
-- same base state gives the same result as legacy's sequential IS-NULL-guarded execution.
dpr_string_name_round1 AS (
    SELECT DISTINCT ON (base.magency, base.projectid)
        base.maprojid,
        b.wkb_geometry AS geom
    FROM base
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON UPPER(base.description) LIKE UPPER('%' || b.signname || '%')
    WHERE
        base.geom IS NULL
        AND base.magency = '846'
        AND UPPER(b.signname) NOT IN ('PARK', 'LOT', 'GARDEN', 'TRIANGLE', 'SITTING AREA', 'BRIDGE PARK')
    ORDER BY base.magency ASC, base.projectid ASC, LENGTH(b.signname) DESC, b.signname ASC
),

dpr_string_name_round2 AS (
    SELECT DISTINCT ON (base.magency, base.projectid)
        base.maprojid,
        b.wkb_geometry AS geom
    FROM base
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON UPPER(base.description) LIKE UPPER('%' || b.signname || '%')
    WHERE
        base.geom IS NULL
        AND base.magency = '846'
        AND UPPER(b.signname) NOT IN ('PARK', 'LOT', 'GARDEN', 'TRIANGLE', 'SITTING AREA')
    ORDER BY base.magency ASC, base.projectid ASC, LENGTH(b.signname) DESC, b.signname ASC
),

dpr_string_name_round3 AS (
    SELECT DISTINCT ON (base.magency, base.projectid)
        base.maprojid,
        b.wkb_geometry AS geom
    FROM base
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON LEVENSHTEIN(UPPER(base.description), UPPER('%' || b.signname || '%')) <= 3
    WHERE
        base.geom IS NULL
        AND base.magency = '846'
        AND UPPER(b.signname) NOT IN ('PARK', 'LOT', 'GARDEN', 'TRIANGLE', 'SITTING AREA', 'BRIDGE PARK')
    ORDER BY
        base.magency ASC, base.projectid ASC,
        LEVENSHTEIN(UPPER(base.description), UPPER('%' || b.signname || '%')) ASC,
        LENGTH(b.signname) DESC, b.signname ASC
),

dpr_string_name AS (
    SELECT
        base.maprojid,
        COALESCE(round1.geom, round2.geom, round3.geom) AS geom,
        'Algorithm'::text AS geomsource,
        'DPR'::text AS datasource,
        'dpr_parksproperties'::text AS dataname
    FROM base
    LEFT JOIN dpr_string_name_round1 AS round1 ON base.maprojid = round1.maprojid
    LEFT JOIN dpr_string_name_round2 AS round2 ON base.maprojid = round2.maprojid
    LEFT JOIN dpr_string_name_round3 AS round3 ON base.maprojid = round3.maprojid
    WHERE round1.geom IS NOT NULL OR round2.geom IS NOT NULL OR round3.geom IS NOT NULL
)

SELECT
    base.ccpversion,
    base.maprojid,
    base.magency,
    base.magencyacro,
    base.projectid,
    base.description,
    base.typecategory,
    COALESCE(base.geomsource, dpr_string_id.geomsource, dpr_string_name.geomsource) AS geomsource,
    COALESCE(base.dataname, dpr_string_id.dataname, dpr_string_name.dataname) AS dataname,
    COALESCE(base.datasource, dpr_string_id.datasource, dpr_string_name.datasource) AS datasource,
    base.datadate,
    COALESCE(base.geom, dpr_string_id.geom, dpr_string_name.geom) AS geom
FROM base
LEFT JOIN dpr_string_id ON base.maprojid = dpr_string_id.maprojid
LEFT JOIN dpr_string_name ON base.maprojid = dpr_string_name.maprojid
