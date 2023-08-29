-- 3 populate the fields that where values are aggregated
WITH primesums AS (
    SELECT
        billingbbl AS primebbl,
        SUM(commercialarea::double precision) AS commercialarea,
        SUM(residarea::double precision) AS residarea,
        SUM(officearea::double precision) AS officearea,
        SUM(retailarea::double precision) AS retailarea,
        SUM(garagearea::double precision) AS garagearea,
        SUM(storagearea::double precision) AS storagearea,
        SUM(factoryarea::double precision) AS factoryarea,
        SUM(otherarea::double precision) AS otherarea
    FROM pluto_input_cama
    WHERE bldgnum = '1' AND billingbbl::numeric > 0
    GROUP BY billingbbl
)

UPDATE pluto a
SET
    comarea = b.commercialarea,
    resarea = b.residarea,
    officearea = b.officearea,
    retailarea = b.retailarea,
    garagearea = b.garagearea,
    strgearea = b.storagearea,
    factryarea = b.factoryarea,
    otherarea = b.otherarea
FROM primesums AS b
WHERE
    a.bbl = b.primebbl
    AND a.lot LIKE '75%'
    AND (
        b.commercialarea::numeric > 0
        OR b.residarea::numeric > 0
        OR b.officearea::numeric > 0
        OR b.retailarea::numeric > 0
        OR b.garagearea::numeric > 0
        OR b.storagearea::numeric > 0
        OR b.factoryarea::numeric > 0
        OR b.otherarea::numeric > 0
    )
    AND (
        a.comarea IS NULL
        AND a.resarea IS NULL
        AND a.officearea IS NULL
        AND a.retailarea IS NULL
        AND a.garagearea IS NULL
        AND a.strgearea IS NULL
        AND a.factryarea IS NULL
        AND a.otherarea IS NULL
    );
