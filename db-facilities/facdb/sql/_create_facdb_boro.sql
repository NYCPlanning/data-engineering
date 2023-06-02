DROP TABLE IF EXISTS facdb_boro;
SELECT
	uid, boro, borocode,
	(CASE
		WHEN boro = 'MANHATTAN' THEN 'NEW YORK'
		WHEN boro IN ('BROOKLYN', 'BRONX', 'STATEN ISLAND') THEN boro
		ELSE city
	END) as city,
    NULLIF(NULLIF(regexp_replace(LEFT(zipcode, 5), '[^0-9]+', '', 'g'), '0'), '') as zipcode
INTO facdb_boro
FROM (
    SELECT
        a.uid,
        b.boroname as boro,
        b.borocode,
        a.city,
        a.zipcode
    FROM (
    SELECT
        uid,
        UPPER(coalesce(b.county, a.boro)) as boro,
        input_zipcode as zipcode,
        UPPER(b.po_name) as city
    FROM (
        SELECT
            uid, UPPER(boro) as boro, borocode, geo_1b->'inputs'->>'input_zipcode' as input_zipcode
        FROM facdb_base
        WHERE uid not in (SELECT uid FROM facdb_spatial where borocode IS NOT NULL)
    ) a LEFT JOIN doitt_zipcodeboundaries b
    ON a.input_zipcode = b.zipcode) a
    LEFT JOIN lookup_boro b
    ON a.boro = b.boro
    UNION
    SELECT
        uid, b.boroname, a.borocode, city, zipcode
    FROM facdb_spatial a
    LEFT JOIN lookup_boro b
    ON b.borocode = a.borocode
    WHERE a.borocode IS NOT NULL
) a
