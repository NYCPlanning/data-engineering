{{ config(
    materialized = 'table'
) }}

WITH int_sedat AS (
    SELECT * FROM {{ ref("int_sedat") }}
)

SELECT 
    -- Complete 87-character fixed-width record
    CONCAT(
        SUBSTR(LPAD(COALESCE(LIONKEY, ''), 10, ' '), 1, 10),
        SUBSTR(COALESCE(PARITY, ' '), 1, 1),
        SUBSTR(RPAD(COALESCE(STREET_NAME, ''), 32, ' '), 1, 32),
        SUBSTR(COALESCE(SIDE_OF_STREET, ' '), 1, 1),
        SUBSTR(LPAD(COALESCE(LOWADDRESS, ''), 7, ' '), 1, 7),
        SUBSTR(RPAD(COALESCE(LOW_ADDR_SUFFIX, ''), 8, ' '), 1, 8),
        SUBSTR(LPAD(COALESCE(HIGHADDRESS, ''), 7, ' '), 1, 7),
        SUBSTR(RPAD(COALESCE(HIGH_ADDR_SUFFIX, ''), 8, ' '), 1, 8),
        SUBSTR(LPAD(COALESCE(ELECTION_DISTRICT, ''), 3, ' '), 1, 3),
        SUBSTR(LPAD(COALESCE(ASSEMBLY_DISTRICT, ''), 2, ' '), 1, 2),
        SUBSTR(RPAD(COALESCE(PREFERRED_B7SC, ''), 8, ' '), 1, 8)
    ) as SEDAT_RECORD
FROM int_sedat
