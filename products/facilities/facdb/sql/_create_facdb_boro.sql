DROP TABLE IF EXISTS facdb_boro;
WITH coalesced_boro AS (
    SELECT
        facdb_zipcode.uid,
        lookup_boro.boroname AS boro,
        lookup_boro.borocode,
        facdb_zipcode.city,
        facdb_zipcode.zipcode
    FROM (
        SELECT
            uid,
            upper(coalesce(zipcodes.county, facdb_boro.boro)) AS boro,
            input_zipcode AS zipcode,
            upper(zipcodes.po_name) AS city
        FROM (
            SELECT
                uid,
                upper(boro) AS boro,
                borocode,
                geo_1b -> 'inputs' ->> 'input_zipcode' AS input_zipcode
            FROM facdb_base
            WHERE uid NOT IN (
                SELECT uid FROM facdb_spatial
                WHERE borocode IS NOT NULL
            )
        ) AS facdb_boro LEFT JOIN doitt_zipcodeboundaries AS zipcodes
            ON facdb_boro.input_zipcode = zipcodes.zipcode
    ) AS facdb_zipcode
    LEFT JOIN lookup_boro
        ON facdb_zipcode.boro = lookup_boro.boro
    UNION
    SELECT
        uid,
        lookup_boro.boroname,
        facdb_spatial.borocode,
        city,
        zipcode
    FROM facdb_spatial
    LEFT JOIN lookup_boro
        ON facdb_spatial.borocode = lookup_boro.borocode
    WHERE facdb_spatial.borocode IS NOT NULL
)
SELECT
    uid,
    boro,
    borocode,
    (CASE
        WHEN boro = 'MANHATTAN' THEN 'NEW YORK'
        WHEN boro IN ('BROOKLYN', 'BRONX', 'STATEN ISLAND') THEN boro
        ELSE city
    END) AS city,
    nullif(nullif(regexp_replace(left(zipcode, 5), '[^0-9]+', '', 'g'), '0'), '') AS zipcode
INTO facdb_boro
FROM coalesced_boro
