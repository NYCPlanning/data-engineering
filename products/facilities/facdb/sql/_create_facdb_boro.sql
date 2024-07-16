DROP TABLE IF EXISTS facdb_boro;
SELECT
    uid,
    boro,
    borocode,
    (CASE
        WHEN boro = 'MANHATTAN' THEN 'NEW YORK'
        WHEN boro IN ('BROOKLYN', 'BRONX', 'STATEN ISLAND') THEN boro
        ELSE city
    END) AS city,
    NULLIF(NULLIF(REGEXP_REPLACE(LEFT(zipcode, 5), '[^0-9]+', '', 'g'), '0'), '') AS zipcode
INTO facdb_boro
FROM (
    SELECT
        a.uid,
        b.boroname AS boro,
        b.borocode,
        a.city,
        a.zipcode
    FROM (
        SELECT
            uid,
            UPPER(COALESCE(b.county, a.boro)) AS boro,
            input_zipcode AS zipcode,
            UPPER(b.po_name) AS city
        FROM (
            SELECT
                uid,
                UPPER(boro) AS boro,
                borocode,
                geo_1b -> 'inputs' ->> 'input_zipcode' AS input_zipcode
            FROM facdb_base
            WHERE uid NOT IN (SELECT uid FROM facdb_spatial WHERE borocode IS NOT NULL)
        ) AS a LEFT JOIN doitt_zipcodeboundaries AS b
            ON a.input_zipcode = b.zipcode
    ) AS a
    LEFT JOIN lookup_boro AS b
        ON a.boro = b.boro
    UNION
    SELECT
        uid,
        b.boroname,
        a.borocode,
        city,
        zipcode
    FROM facdb_spatial AS a
    LEFT JOIN lookup_boro AS b
        ON a.borocode = b.borocode
    WHERE a.borocode IS NOT NULL
) AS a
