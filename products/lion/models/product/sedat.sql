{{ config(
    materialized = 'table'
) }}

WITH int_sedat AS (
    SELECT * FROM {{ ref("int_sedat") }}
)

SELECT
    -- Complete 87-character fixed-width record
    CONCAT(
        SUBSTR(LPAD(COALESCE(lionkey, ''), 10, ' '), 1, 10),
        SUBSTR(COALESCE(parity, ' '), 1, 1),
        SUBSTR(RPAD(COALESCE(street_name, ''), 32, ' '), 1, 32),
        SUBSTR(COALESCE(side_of_street, ' '), 1, 1),
        SUBSTR(LPAD(COALESCE(lowaddress, ''), 7, ' '), 1, 7),
        SUBSTR(RPAD(COALESCE(low_addr_suffix, ''), 8, ' '), 1, 8),
        SUBSTR(LPAD(COALESCE(highaddress, ''), 7, ' '), 1, 7),
        SUBSTR(RPAD(COALESCE(high_addr_suffix, ''), 8, ' '), 1, 8),
        SUBSTR(LPAD(COALESCE(election_district, ''), 3, ' '), 1, 3),
        SUBSTR(LPAD(COALESCE(assembly_district, ''), 2, ' '), 1, 2),
        SUBSTR(RPAD(COALESCE(preferred_b7sc, ''), 8, ' '), 1, 8)
    ) AS sedat_record
FROM int_sedat
