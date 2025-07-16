-- calculate how much (total area and percentage) of each lot is covered by a special purpose district
-- assign the special purpose district(s) to each tax lot
-- the order special purpose districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the lot is covered by the district
-- OR if no lot meets the 10% threshold, than the lot with the highest percentage of the special district is assigned it
DROP TABLE IF EXISTS specialpurposeper;
CREATE TABLE specialpurposeper AS
SELECT
    p.id,
    p.bbl,
    n.sdlbl,
    ST_Area(
        CASE
            WHEN ST_CoveredBy(p.geom, n.geom) THEN p.geom
            ELSE ST_Multi(ST_Intersection(p.geom, n.geom))
        END
    ) AS segbblgeom,
    ST_Area(p.geom) AS allbblgeom,
    ST_Area(
        CASE
            WHEN ST_CoveredBy(n.geom, p.geom) THEN n.geom
            ELSE ST_Multi(ST_Intersection(n.geom, p.geom))
        END
    ) AS segzonegeom,
    ST_Area(n.geom) AS allzonegeom
FROM pluto AS p
INNER JOIN dcp_specialpurpose AS n
    ON ST_Intersects(p.geom, n.geom);

DROP TABLE IF EXISTS specialpurposeperorder;
CREATE TABLE specialpurposeperorder AS
WITH specialpurposeperorder_init AS (
    SELECT
        id,
        bbl,
        sdlbl,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        -- per sp district type, rank by 
        --   1) if lot meets 10% coverage by sp district threshold
        --   2) area of coverage
        -- This is to get cases where special sp district may only be assigned to one lot. 
        -- 1) is to cover edge case where largest section of specific large (but common) sp district that happens to have largest area on small fraction of huge lot
        -- See BBL 1013730001 in ZoLa - largest section of special district but should not be assigned spdist SRI
        row_number()
            OVER (
                PARTITION BY sdlbl
                ORDER BY (segbblgeom / allbblgeom) * 100 < 10, segzonegeom DESC
            )
        AS sd_row_number
    FROM specialpurposeper
)

SELECT
    id,
    bbl,
    sdlbl,
    segbblgeom,
    row_number() OVER (
        PARTITION BY id
        ORDER BY segbblgeom DESC, sdlbl ASC
    ) AS row_number
FROM specialpurposeperorder_init
WHERE
    perbblgeom >= 10
    OR sd_row_number = 1;

UPDATE pluto a
SET spdist1 = sdlbl
FROM specialpurposeperorder AS b
WHERE
    a.id = b.id
    AND row_number = 1;

UPDATE pluto a
SET spdist2 = sdlbl
FROM specialpurposeperorder AS b
WHERE
    a.id = b.id
    AND row_number = 2;

UPDATE pluto a
SET spdist3 = sdlbl
FROM specialpurposeperorder AS b
WHERE
    a.id = b.id
    AND row_number = 3;

-- set the order of special districts 
UPDATE pluto
SET
    spdist1 = 'CL',
    spdist2 = 'MiD'
WHERE
    spdist1 = 'MiD'
    AND spdist2 = 'CL';
UPDATE pluto
SET
    spdist1 = 'MiD',
    spdist2 = 'TA'
WHERE
    spdist1 = 'TA'
    AND spdist2 = 'MiD';
UPDATE pluto
SET
    spdist1 = '125th',
    spdist2 = 'TA'
WHERE
    spdist1 = 'TA'
    AND spdist2 = '125th';
UPDATE pluto
SET
    spdist1 = 'EC-5',
    spdist2 = 'MX-16'
WHERE
    spdist1 = 'MX-16'
    AND spdist2 = 'EC-5';
UPDATE pluto
SET
    spdist1 = 'EC-6',
    spdist2 = 'MX-16'
WHERE
    spdist1 = 'MX-16'
    AND spdist2 = 'EC-6';
UPDATE pluto
SET
    spdist1 = 'EHC',
    spdist2 = 'TA'
WHERE
    spdist1 = 'TA'
    AND spdist2 = 'EHC';
UPDATE pluto
SET
    spdist1 = 'MX-1',
    spdist2 = 'G'
WHERE
    spdist1 = 'G'
    AND spdist2 = 'MX-1';
