-- geometries found by matching a project's description against park and facility names.
-- Each match is limited to some agencies, and each takes one geometry per project.
WITH projects AS (
    SELECT
        maprojid,
        magency,
        magencyacro,
        projectid,
        description
    FROM {{ ref('int__ccp_projects') }}
),

-- DPR park ID in the description, like "Q123"
dpr_park_id AS (
    SELECT DISTINCT ON (p.maprojid)
        p.maprojid,
        'dpr_park_id'::text AS candidate,
        'dpr'::text AS geomsource,
        'dpr'::text AS datasource,
        'dpr_parksproperties'::text AS dataname,
        b.wkb_geometry AS geom
    FROM projects AS p
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON SUBSTRING(p.description FROM '[BMQRX][0-9][0-9][0-9]') = b.gispropnum
    WHERE p.magencyacro = 'DPR'
    ORDER BY p.maprojid ASC, b.gispropnum ASC
),

-- DPR park name in the description, in 3 rounds: round 1 excludes generic names including
-- 'BRIDGE PARK', round 2 allows 'BRIDGE PARK', round 3 excludes it again but matches by
-- levenshtein distance instead of LIKE
dpr_park_name_round1 AS (
    SELECT DISTINCT ON (p.magency, p.projectid)
        p.maprojid,
        'dpr_park_name_round1'::text AS candidate,
        b.wkb_geometry AS geom
    FROM projects AS p
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON UPPER(p.description) LIKE UPPER('%' || b.signname || '%')
    WHERE
        p.magency = '846'
        AND UPPER(b.signname) NOT IN ('PARK', 'LOT', 'GARDEN', 'TRIANGLE', 'SITTING AREA', 'BRIDGE PARK')
    ORDER BY p.magency ASC, p.projectid ASC, LENGTH(b.signname) DESC, b.signname ASC
),

dpr_park_name_round2 AS (
    SELECT DISTINCT ON (p.magency, p.projectid)
        p.maprojid,
        'dpr_park_name_round2'::text AS candidate,
        b.wkb_geometry AS geom
    FROM projects AS p
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON UPPER(p.description) LIKE UPPER('%' || b.signname || '%')
    WHERE
        p.magency = '846'
        AND UPPER(b.signname) NOT IN ('PARK', 'LOT', 'GARDEN', 'TRIANGLE', 'SITTING AREA')
    ORDER BY p.magency ASC, p.projectid ASC, LENGTH(b.signname) DESC, b.signname ASC
),

dpr_park_name_round3 AS (
    SELECT DISTINCT ON (p.magency, p.projectid)
        p.maprojid,
        'dpr_park_name_round3'::text AS candidate,
        b.wkb_geometry AS geom
    FROM projects AS p
    INNER JOIN {{ ref('stg__dpr_parksproperties') }} AS b
        ON LEVENSHTEIN(UPPER(p.description), UPPER('%' || b.signname || '%')) <= 3
    WHERE
        p.magency = '846'
        AND UPPER(b.signname) NOT IN ('PARK', 'LOT', 'GARDEN', 'TRIANGLE', 'SITTING AREA', 'BRIDGE PARK')
    ORDER BY
        p.magency ASC, p.projectid ASC,
        LEVENSHTEIN(UPPER(p.description), UPPER('%' || b.signname || '%')) ASC,
        LENGTH(b.signname) DESC, b.signname ASC
),

dpr_park_name AS (
    SELECT
        maprojid,
        candidate,
        'Algorithm'::text AS geomsource,
        'DPR'::text AS datasource,
        'dpr_parksproperties'::text AS dataname,
        geom
    FROM (
        SELECT * FROM dpr_park_name_round1
        UNION ALL
        SELECT * FROM dpr_park_name_round2
        UNION ALL
        SELECT * FROM dpr_park_name_round3
    ) AS rounds
),

-- facility name in the description, limited to names that are unique citywide and have at
-- least 3 words. Both sides are filtered before the join, which compares every pair.
facilities_unique_names AS (
    SELECT facname
    FROM {{ ref('stg__dcp_facilities') }}
    WHERE facname LIKE '% % %'
    GROUP BY facname
    HAVING COUNT(facname) = 1
),

facilities_matchable AS (
    SELECT
        b.facname,
        UPPER(b.facname) AS facname_upper,
        b.wkb_geometry
    FROM {{ ref('stg__dcp_facilities') }} AS b
    INNER JOIN facilities_unique_names AS u ON b.facname = u.facname
),

facility_name_projects AS (
    SELECT
        maprojid,
        UPPER(description) AS description_upper
    FROM projects
    WHERE magency IN (
        '850', '801', '806', '126', '819', '57', '72', '858', '827', '71', '56', '816', '125', '998'
    )
),

facility_name AS (
    SELECT DISTINCT ON (p.maprojid)
        p.maprojid,
        'facility_name'::text AS candidate,
        b.wkb_geometry AS geom
    FROM facility_name_projects AS p
    INNER JOIN facilities_matchable AS b
        ON p.description_upper LIKE '%' || b.facname_upper || '%'
    ORDER BY p.maprojid ASC, LENGTH(b.facname) DESC, b.facname ASC
),

-- library name in the description, without the uniqueness limit
library_name AS (
    SELECT DISTINCT ON (p.maprojid)
        p.maprojid,
        'library_name'::text AS candidate,
        b.wkb_geometry AS geom
    FROM projects AS p
    INNER JOIN {{ ref('stg__dcp_facilities') }} AS b
        ON UPPER(p.description) LIKE '%' || UPPER(b.facname) || '%'
    WHERE
        p.magency::int IN (39, 37, 38, 35)
        AND UPPER(p.description) NOT LIKE '%AND%'
        AND b.facgroup = 'Libraries'
    ORDER BY p.maprojid ASC, LENGTH(b.facname) DESC, b.facname ASC
),

facilities AS (
    SELECT
        maprojid,
        candidate,
        'Facilities database'::text AS geomsource,
        NULL::text AS datasource,
        NULL::text AS dataname,
        geom
    FROM (
        SELECT * FROM facility_name
        UNION ALL
        SELECT * FROM library_name
    ) AS matches
)

SELECT * FROM dpr_park_id
UNION ALL
SELECT * FROM dpr_park_name
UNION ALL
SELECT * FROM facilities
