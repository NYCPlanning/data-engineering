-- set building class for condo lots if there is only one unique building class value
WITH bldgclass AS (
    SELECT DISTINCT
        billingbbl,
        bldgcl,
        row_number()
            OVER (
                PARTITION BY billingbbl
                ORDER BY bldgcl
            )
        AS row_number
    FROM (
        SELECT DISTINCT
            billingbbl,
            bldgcl
        FROM pluto_rpad_geo
        WHERE
            bldgcl != 'R0'
            AND bldgcl != 'RG'
            AND bldgcl != 'RP'
            AND bldgcl != 'RT'
            AND billingbbl::numeric > 0
    ) AS x
),

maxnum AS (
    SELECT
        billingbbl,
        max(row_number) AS maxrow_number
    FROM bldgclass
    GROUP BY billingbbl
)

UPDATE pluto a
SET
    bldgclass = (CASE
        WHEN c.maxrow_number = 1 THEN b.bldgcl
    END)
FROM bldgclass AS b, maxnum AS c
WHERE
    a.bbl = b.billingbbl
    AND c.billingbbl = a.bbl;

-- set building class for condo lots where there are multiple building class values
CREATE TEMP TABLE bblsbldgclasslookup AS (
    WITH bldgclass AS (
        SELECT DISTINCT
            billingbbl,
            bldgcl,
            row_number()
                OVER (
                    PARTITION BY billingbbl
                    ORDER BY bldgcl
                )
            AS row_number
        FROM (
            SELECT DISTINCT
                billingbbl,
                bldgcl
            FROM pluto_rpad_geo
            WHERE
                bldgcl != 'R0'
                AND bldgcl != 'RG'
                AND bldgcl != 'RP'
                AND bldgcl != 'RT'
                AND billingbbl::numeric > 0
        ) AS x
    ),

    maxnum AS (
        SELECT
            billingbbl,
            max(row_number) AS maxrow_number
        FROM bldgclass
        GROUP BY billingbbl
    ),

    bldgclassmed AS (
        SELECT
            a.billingbbl,
            a.bldgcl,
            a.row_number,
            c.type AS bldg_type
        FROM bldgclass AS a, maxnum AS b, pluto_input_condo_bldgclass AS c
        WHERE
            b.maxrow_number >= 2
            AND a.billingbbl = b.billingbbl
            AND a.bldgcl = c.code
        ORDER BY a.billingbbl
    )

    SELECT DISTINCT
        billingbbl,
        bldg_type
    FROM bldgclassmed
    ORDER BY billingbbl, bldg_type
);

CREATE TEMP TABLE bblsbldgclass AS (
    SELECT
        billingbbl,
        string_agg(bldg_type, ', ') AS bldg_type
    FROM bblsbldgclasslookup
    GROUP BY billingbbl
);

UPDATE pluto a
SET bldgclass = 'RD'
FROM bblsbldgclass AS b
WHERE
    a.bbl = b.billingbbl
    AND bldg_type = 'Res';
UPDATE pluto a
SET bldgclass = 'RC'
FROM bblsbldgclass AS b
WHERE
    a.bbl = b.billingbbl
    AND bldg_type = 'Com';
UPDATE pluto a
SET bldgclass = 'RI'
FROM bblsbldgclass AS b
WHERE
    a.bbl = b.billingbbl
    AND bldg_type = 'Com, Ind';
UPDATE pluto a
SET bldgclass = 'RM'
FROM bblsbldgclass AS b
WHERE
    a.bbl = b.billingbbl
    AND (bldg_type = 'Com, Res' OR (bldg_type LIKE '%R9%' AND bldg_type NOT LIKE '%Ind%'));
UPDATE pluto a
SET bldgclass = 'RX'
FROM bblsbldgclass AS b
WHERE
    a.bbl = b.billingbbl
    AND (bldg_type = 'Com, Ind, Res' OR bldg_type LIKE '%Ind, R9%');
UPDATE pluto a
SET bldgclass = 'RZ'
FROM bblsbldgclass AS b
WHERE
    a.bbl = b.billingbbl
    AND bldg_type = 'Ind, Res';

-- set the building class to Q0 
-- where the zoning district is park and the building class is null or vacant
UPDATE pluto
SET bldgclass = 'Q0'
WHERE
    zonedist1 = 'PARK'
    AND (bldgclass IS NULL OR bldgclass LIKE 'V%');

-- set the building class to QG 
-- where the building class is null or vacant
-- and more than 15% of the lot is covered by a greenthumb garden
-- or more than 25% of the garden is in a lot
WITH
gardenlayper AS (
    SELECT
        p.bbl,
        st_area(
            CASE
                WHEN st_coveredby(p.geom, n.geom)
                    THEN p.geom
                ELSE st_multi(st_intersection(p.geom, n.geom))
            END
        ) AS segbblgeom,
        st_area(p.geom) AS allbblgeom,
        st_area(
            CASE
                WHEN st_coveredby(n.geom, p.geom)
                    THEN n.geom
                ELSE st_multi(st_intersection(n.geom, p.geom))
            END
        ) AS segzonegeom,
        st_area(n.geom) AS allzonegeom
    FROM pluto AS p
    INNER JOIN dpr_greenthumb AS n
        ON st_intersects(p.geom, n.geom)
    WHERE p.bldgclass LIKE 'V%' OR p.bldgclass IS NULL
),

bblsbldgclasslookupgardens AS (
    SELECT
        bbl,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS pergardengeom
    FROM gardenlayper
)

UPDATE pluto a
SET bldgclass = 'QG'
FROM bblsbldgclasslookupgardens AS b
WHERE
    a.bbl = b.bbl
    AND (
        perbblgeom >= 15
        OR pergardengeom >= 25
    );

-- update Z7 values
WITH
z7s AS (
    SELECT bbl FROM pluto
    WHERE bldgclass = 'Z7'
),

bldgclass AS (
    SELECT DISTINCT
        bbl,
        bldgcl,
        row_number()
            OVER (
                PARTITION BY bbl
                ORDER BY bldgcl
            )
        AS row_number
    FROM (
        SELECT DISTINCT
            b.bldgcl,
            b.boro || b.tb || b.tl AS bbl
        FROM z7s AS a
        LEFT JOIN pluto_rpad_geo AS b
            ON a.bbl = b.boro || b.tb || b.tl
    ) AS x
),

bblsbldgclasslookup AS (
    SELECT
        bbl,
        bldgcl
    FROM bldgclass
    WHERE row_number = 1
)

UPDATE pluto a
SET bldgclass = b.bldgcl
FROM bblsbldgclasslookup AS b
WHERE
    a.bbl = b.bbl
    AND a.bldgclass = 'Z7';
