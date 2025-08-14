{{ config(
    materialized = 'table'
) }}

WITH int_special_sedat AS (
    SELECT * FROM {{ ref("int_special_sedat") }}
    WHERE MISSING_STREET_NAME = FALSE
)

SELECT 
    CONCAT(
        LPAD(COALESCE(LIONKEY, ''), 10, ' '),     
        SUBSTR(COALESCE(PARITY, ' '), 1, 1),     
        RPAD(COALESCE(STREET_NAME, ''), 32, ' '),     
        SUBSTR(COALESCE(SIDE_OF_STREET, ' '), 1, 1), 
        LPAD(COALESCE(LOWADDRESS, ''), 7, ' '),             
        RPAD(COALESCE(LOW_ADDR_SUFFIX, ''), 8, ' '),        
        LPAD(COALESCE(HIGHADDRESS, ''), 7, ' '),           
        RPAD(COALESCE(HIGH_ADDR_SUFFIX, ''), 8, ' '),       
        LPAD(COALESCE(ELECTION_DISTRICT, ''), 3, '0'),       
        LPAD(COALESCE(ASSEMBLY_DISTRICT, ''), 2, '0'),       
        SUBSTR(RPAD(COALESCE(B7SC, ''), 8, ' '), 1, 8)                    
    ) as SPECIAL_SEDAT_RECORD
FROM int_special_sedat
