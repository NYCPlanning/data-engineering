/*
DESCRIPTION: Translates IPIS BBL into the appropriate BBL for geocode with function BL.
    In most cases, ipis_bbl and geo_bbl are the same. If the IPIS BBL is an air rights lot,
    the geocoding input BBL is an associated donating BBL, as described in the DOF DTM air rights
    table. This table is not a one-to-one lookup. When identifying the appropriate donating BBL:
        - '89' lots aren't considered
        - If an air rights lot matches to more than one non-89 donating BBL, we use the donating BBL
          that shares the last 3 digits as the air rights BBL

INPUTS:

    dcas_ipis (
        * bbl,
        house_number,
        street_name,
        bbl
    )

    dof_air_rights_lots (
        donating_bbl,
        air_rights_bbl
    )

OUTPUTS:
    geo_inputs (
        uid,
        house_number,
        street_name,
        ipis_bbl,
        geo_bbl
    )
*/

DROP TABLE IF EXISTS geo_inputs CASCADE;
WITH
-- When identifying donating BBLs for air rights lots, do not include 89 lots
air_rights_counts AS (
    SELECT
        a.air_lot_bbl AS air_rights_bbl,
        a.parent_bbl AS donating_bbl,
        b.lot_count
    FROM dof_air_rights_lots AS a
    INNER JOIN (
        SELECT
            air_lot_bbl,
            count(*) AS lot_count
        FROM dof_air_rights_lots
        GROUP BY air_lot_bbl
    ) AS b
        ON a.air_lot_bbl = b.air_lot_bbl
    WHERE substring(parent_bbl, 7, 2) <> '89'
)
-- If an air rights BBL matches with multiple donating BBLs, take the lot with matching last 3 digits
SELECT
    md5(cast((a.*) AS text)) AS uid,
    a.house_number,
    a.street_name,
    a.bbl AS ipis_bbl,
    coalesce(b.donating_bbl, a.bbl) AS geo_bbl
INTO geo_inputs
FROM dcas_ipis AS a
LEFT JOIN
    (
        SELECT
            air_rights_bbl,
            donating_bbl
        FROM air_rights_counts
        WHERE lot_count = 1
        UNION
        SELECT
            air_rights_bbl,
            donating_bbl
        FROM air_rights_counts
        WHERE
            lot_count > 1
            AND right(air_rights_bbl, 3) = right(donating_bbl, 3)
    ) AS b
    ON a.bbl = b.air_rights_bbl;
