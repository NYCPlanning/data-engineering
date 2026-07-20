WITH base AS (
    SELECT * FROM {{ ref('int__dcpattributes_geom_stringmatch') }}
),

-- gap-fill: general facilities, restricted to facility names that are unique citywide and have
-- at least 3 words (cheap disambiguation heuristic in the legacy SQL)
facilities_singlename AS (
    SELECT facname
    FROM {{ ref('stg__dcp_facilities') }}
    GROUP BY facname
    HAVING COUNT(facname) = 1
),

facilities_master AS (
    SELECT DISTINCT ON (base.maprojid)
        base.maprojid,
        b.wkb_geometry AS geom
    FROM base
    INNER JOIN {{ ref('stg__dcp_facilities') }} AS b
        ON UPPER(base.description) LIKE '%' || UPPER(b.facname) || '%'
    WHERE
        base.geom IS NULL
        AND base.magency IN (
            '850', '801', '806', '126', '819', '57', '72', '858', '827', '71', '56', '816', '125', '998'
        )
        AND b.facname LIKE '% % %'
        AND b.facname IN (SELECT facname FROM facilities_singlename)
    ORDER BY base.maprojid ASC, LENGTH(b.facname) DESC, b.facname ASC
),

-- gap-fill: libraries specifically, no city-wide-uniqueness constraint on facname
facilities_lib_master AS (
    SELECT DISTINCT ON (base.maprojid)
        base.maprojid,
        b.wkb_geometry AS geom
    FROM base
    INNER JOIN {{ ref('stg__dcp_facilities') }} AS b
        ON UPPER(base.description) LIKE '%' || UPPER(b.facname) || '%'
    WHERE
        base.geom IS NULL
        AND base.magency::int IN (39, 37, 38, 35)
        AND UPPER(base.description) NOT LIKE '%AND%'
        AND b.facgroup = 'Libraries'
    ORDER BY base.maprojid ASC, LENGTH(b.facname) DESC, b.facname ASC
),

facilities AS (
    SELECT
        base.maprojid,
        COALESCE(facilities_master.geom, facilities_lib_master.geom) AS geom,
        'Facilities database'::text AS geomsource
    FROM base
    LEFT JOIN facilities_master ON base.maprojid = facilities_master.maprojid
    LEFT JOIN facilities_lib_master ON base.maprojid = facilities_lib_master.maprojid
    WHERE facilities_master.geom IS NOT NULL OR facilities_lib_master.geom IS NOT NULL
)

SELECT
    base.ccpversion,
    base.maprojid,
    base.magency,
    base.magencyacro,
    base.projectid,
    base.description,
    base.typecategory,
    COALESCE(facilities.geomsource, base.geomsource) AS geomsource,
    base.dataname,
    base.datasource,
    base.datadate,
    -- attributes_maprojid_parkid.sql's ST_MAKEVALID(geom) cleanup applies unconditionally to
    -- every row at this point in the legacy chain, not just facilities-matched ones
    ST_MAKEVALID(COALESCE(facilities.geom, base.geom)) AS geom
FROM base
LEFT JOIN facilities ON base.maprojid = facilities.maprojid
