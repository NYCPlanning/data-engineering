-- calculate how much (total area and percentage) of each lot is covered by a zoning district
-- assign the zoning district(s) to each tax lot
-- the order zoning districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR the majority of the district is within the lot


DROP TABLE IF EXISTS validdtm;
CREATE TABLE validdtm AS (
    SELECT
        id,
        bbl,
        ST_MAKEVALID(geom) AS geom
    FROM pluto
    WHERE ST_GEOMETRYTYPE(ST_MAKEVALID(geom)) = 'ST_MultiPolygon'
);
CREATE INDEX validdtm_geom_idx ON validdtm USING gist (geom gist_geometry_ops_2d);

ANALYZE validdtm;

DROP TABLE IF EXISTS validzones;
CREATE TABLE validzones AS (
    SELECT
        CASE
            WHEN zonedist = ANY('{"BALL FIELD", "PLAYGROUND", "PUBLIC PLACE"}') THEN 'PARK'
            ELSE zonedist
        END AS zonedist,
        ST_MAKEVALID(geom) AS geom
    FROM dcp_zoningdistricts
    WHERE ST_GEOMETRYTYPE(ST_MAKEVALID(geom)) = 'ST_MultiPolygon'
);

CREATE INDEX validzones_geom_idx ON validzones USING gist (geom gist_geometry_ops_2d);

ANALYZE validzones;

DROP TABLE IF EXISTS lotzoneper;
CREATE TABLE lotzoneper AS
SELECT
    p.id,
    bbl,
    n.zonedist,
    ST_AREA(
        CASE
            WHEN ST_COVEREDBY(p.geom, n.geom) THEN p.geom::geography
            ELSE ST_MULTI(ST_INTERSECTION(p.geom, n.geom))::geography
        END
    ) AS segbblgeom,
    ST_AREA(
        CASE
            WHEN ST_COVEREDBY(n.geom, p.geom) THEN n.geom::geography
            ELSE ST_MULTI(ST_INTERSECTION(n.geom, p.geom))::geography
        END
    ) AS segzonegeom,
    ST_AREA(p.geom::geography) AS allbblgeom,
    ST_AREA(n.geom::geography) AS allzonegeom

FROM validdtm AS p
INNER JOIN validzones AS n
    ON ST_INTERSECTS(p.geom, n.geom);

ALTER TABLE lotzoneper
SET (parallel_workers = 30);

ANALYZE lotzoneper;

-- group by zoning district over lots so that (for example) multiple PARK zones are totalled up
DROP TABLE IF EXISTS lotzoneper_grouped;
CREATE TABLE lotzoneper_grouped AS
SELECT
    id,
    bbl,
    zonedist,
    allbblgeom,
    SUM(segbblgeom) AS segbblgeom,
    SUM(segzonegeom) AS segzonegeom,
    SUM(allzonegeom) AS allzonegeom
FROM lotzoneper
GROUP BY id, bbl, allbblgeom, zonedist;

-- ordered zonedists per lot before applying tie-breaking logic from zonedist_priority
DROP TABLE IF EXISTS lotzoneperorder_init;
CREATE TABLE lotzoneperorder_init AS
WITH initial_rankings AS (
    SELECT
        id,
        bbl,
        zonedist,
        segbblgeom,
        allbblgeom,
        segzonegeom,
        allzonegeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        -- zone districts per lot ranked by percent of lot covered
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY segbblgeom DESC
        ) AS bbl_row_number,
        -- per zoning district type, rank by 
        --   1) if lot meets 10% coverage by zoning district threshold
        --   2) area of coverage
        -- This is to get cases where special zoning district may only be assigned to one lot. 
        -- 1) is to cover edge case where largest section of specific large (but common) zoning district that happens to have largest area on small fraction of huge lot
        ROW_NUMBER() OVER (
            PARTITION BY zonedist
            ORDER BY (segbblgeom / allbblgeom) * 100 < 10, segzonegeom DESC
        ) AS zonedist_row_number
    FROM lotzoneper_grouped
)

SELECT
    id,
    bbl,
    zonedist,
    segbblgeom,
    allbblgeom,
    perbblgeom,
    segzonegeom,
    allzonegeom,
    perzonegeom,
    ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY segbblgeom DESC
    ) AS row_number,
    -- identifying whether zone of rank n for a lot is within 0.01 sq m of zone of rank (n-1) for same lot for tie-breaking logic in next query
    CASE
        WHEN
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY segbblgeom DESC
            ) = 1
            OR LAG(segbblgeom, 1, segbblgeom) OVER (
                PARTITION BY id
                ORDER BY segbblgeom DESC
            ) - segbblgeom > 0.01
            THEN 1
        ELSE 0
    END AS group_start
FROM initial_rankings
WHERE
    bbl_row_number = 1
    OR perbblgeom >= 10
    OR zonedist_row_number = 1;


-- re-assign order based on priorities
-- any groups of assigned zoning districts with within 1% area of bbl of each other should be re-ordered
DROP TABLE IF EXISTS lotzoneperorder;
CREATE TABLE lotzoneperorder AS
WITH group_column_added AS (
    SELECT
        id,
        -- this is not summing by any sql grouping, but rather summing in a window function as the rows are iterated through
        -- output column ends up being a grouping of lot/zone pairings that are "tied" within some limit and should be reordered
        --     based on ranking in zonedist_priority
        zonedist,
        row_number,
        SUM(group_start) OVER (
            PARTITION BY id
            ORDER BY row_number
        ) AS reorder_group
    FROM lotzoneperorder_init
),

reorder_groups AS (
    SELECT
        id,
        reorder_group,
        MIN(row_number) - 1 AS order_start, -- starting point for re-ordering in "new_order" CTE down below
        ARRAY_AGG(zonedist) AS zonedist
    FROM group_column_added
    GROUP BY id, reorder_group
    HAVING COUNT(*) > 1 -- Filter to actual groups and not just individual rows
),

rows_to_reorder AS (
    SELECT
        id,
        reorder_group,
        order_start,
        UNNEST(zonedist) AS zonedist
    FROM reorder_groups
),

new_order AS (
    SELECT
        g.id,
        g.zonedist,
        ROW_NUMBER() OVER (
            PARTITION BY id, reorder_group
            ORDER BY priority ASC
        ) + g.order_start AS row_number
    FROM rows_to_reorder AS g
    INNER JOIN zonedist_priority AS zdp ON g.zonedist = zdp.zonedist
)

SELECT
    a.id,
    a.bbl,
    a.zonedist,
    segbblgeom,
    allbblgeom,
    perbblgeom,
    segzonegeom,
    allzonegeom,
    perzonegeom,
    COALESCE(new.row_number, a.row_number) AS row_number
FROM lotzoneperorder_init AS a
LEFT JOIN new_order AS new ON a.id = new.id AND a.zonedist = new.zonedist;

UPDATE pluto a
SET zonedist1 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.id = b.id
    AND row_number = 1;

UPDATE pluto a
SET zonedist2 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.id = b.id
    AND row_number = 2;

UPDATE pluto a
SET zonedist3 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.id = b.id
    AND row_number = 3;

UPDATE pluto a
SET zonedist4 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.id = b.id
    AND row_number = 4;
