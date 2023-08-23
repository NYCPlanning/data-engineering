DROP TABLE IF EXISTS facdb_address;
WITH
addresses AS (
    SELECT
        uid,
        source,
        addressnum,
        TRIM(regexp_replace(streetname, '\s+', ' ', 'g')) as streetname,
        nullif(geo_1b->'result'->>'geo_house_number','') as geo_house_number,
        nullif(geo_1b->'result'->>'geo_street_name','') as geo_street_name,
        nullif(geo_1b->'result'->>'geo_grc','') as geo_grc,
        nullif(geo_1b->'result'->>'geo_grc2','') as geo_grc2,
        geo_1b->'inputs'->>'input_hnum' as input_hnum,
        geo_1b->'inputs'->>'input_sname' as input_sname,
        TRIM(regexp_replace(address, '\s+', ' ', 'g')) as address
    FROM facdb_base
)
SELECT *
INTO facdb_address
FROM
(
    -- COLP addresses
    SELECT
        uid,
        source,
        (CASE
            WHEN addressnum !~ '[0-9]' THEN NULL
            ELSE addressnum
        END) as addressnum,
        streetname,
        (CASE
            WHEN addressnum !~ '[0-9]' THEN streetname
            ELSE address
        END) as address
    FROM addresses
    WHERE source = 'dcp_colp'
    UNION
    -- Non-COLP geocoded addresses
    SELECT
        uid,
        source,
        geo_house_number as addressnum,
        TRIM(regexp_replace(geo_street_name, '\s+', ' ', 'g')) as streetname,
        TRIM(
            UPPER(
                nullif(
                    CONCAT(
                        geo_house_number,' ',regexp_replace(geo_street_name, '\s+', ' ', 'g')
                    ),
                ' ')
            )
        ) as address
    FROM addresses
    WHERE (source <> 'dcp_colp'
        AND geo_grc IN ('00', '01')
        AND geo_grc2 IN ('00', '01'))
    UNION
    -- Non-COLP, non-geocoded addresses
    SELECT
        uid,
        source,
        NULL as addressnum,
        NULL as streetname,
        (CASE
        	WHEN address ~* ' at '
        		OR address LIKE '%@%'
        		OR address ~* ' and '
        		OR address LIKE '%&%'
        		OR address ~* 'PO BOX'
        	THEN NULL
        	ELSE UPPER(address)
        END) as address
    FROM addresses
    WHERE (source <> 'dcp_colp'
        AND NOT (geo_grc IN ('00', '01')
            AND geo_grc2 IN ('00', '01')))
) a;
