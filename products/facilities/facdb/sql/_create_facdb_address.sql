DROP TABLE IF EXISTS facdb_address;
WITH
addresses AS (
    SELECT
        uid,
        source,
        addressnum,
        TRIM(REGEXP_REPLACE(streetname, '\s+', ' ', 'g')) AS streetname,
        NULLIF(geo_1b -> 'result' ->> 'geo_house_number', '') AS geo_house_number,
        NULLIF(geo_1b -> 'result' ->> 'geo_street_name', '') AS geo_street_name,
        NULLIF(geo_1b -> 'result' ->> 'geo_grc', '') AS geo_grc,
        NULLIF(geo_1b -> 'result' ->> 'geo_grc2', '') AS geo_grc2,
        geo_1b -> 'inputs' ->> 'input_hnum' AS input_hnum,
        geo_1b -> 'inputs' ->> 'input_sname' AS input_sname,
        TRIM(REGEXP_REPLACE(address, '\s+', ' ', 'g')) AS address
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
            END) AS addressnum,
            streetname,
            (CASE
                WHEN addressnum !~ '[0-9]' THEN streetname
                ELSE address
            END) AS address
        FROM addresses
        WHERE source = 'dcp_colp'
        UNION
        -- Non-COLP geocoded addresses
        SELECT
            uid,
            source,
            geo_house_number AS addressnum,
            TRIM(REGEXP_REPLACE(geo_street_name, '\s+', ' ', 'g')) AS streetname,
            TRIM(
                UPPER(
                    NULLIF(
                        CONCAT(
                            geo_house_number, ' ', REGEXP_REPLACE(geo_street_name, '\s+', ' ', 'g')
                        ),
                        ' '
                    )
                )
            ) AS address
        FROM addresses
        WHERE (
            source <> 'dcp_colp'
            AND geo_grc IN ('00', '01')
            AND geo_grc2 IN ('00', '01')
        )
        UNION
        -- Non-COLP, non-geocoded addresses
        SELECT
            uid,
            source,
            NULL AS addressnum,
            NULL AS streetname,
            (CASE
                WHEN
                    address ~* ' at '
                    OR address LIKE '%@%'
                    OR address ~* ' and '
                    OR address LIKE '%&%'
                    OR address ~* 'PO BOX'
                    THEN NULL
                ELSE UPPER(address)
            END) AS address
        FROM addresses
        WHERE (
            source <> 'dcp_colp'
            AND NOT (
                geo_grc IN ('00', '01')
                AND geo_grc2 IN ('00', '01')
            )
        )
    ) AS a;
