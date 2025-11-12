{{ config(
   materialized = 'table'
) }}

SELECT
    CONCAT(
        FORMAT_LION_TEXT(lionkey, 10, ' '),
        COALESCE(parity, ' '),
        FORMAT_LION_TEXT(street_name, 32, ' ', FALSE, TRUE),
        COALESCE(side_of_street, ' '),
        FORMAT_LION_TEXT(lowaddress, 7, ' '),
        FORMAT_LION_TEXT(low_addr_suffix, 8, ' ', FALSE, TRUE),
        FORMAT_LION_TEXT(highaddress, 7, ' '),
        FORMAT_LION_TEXT(high_addr_suffix, 8, ' ', FALSE, TRUE),
        FORMAT_LION_TEXT(election_district, 3, '0'),
        FORMAT_LION_TEXT(assembly_district, 2, '0'),
        FORMAT_LION_TEXT(b7sc, 8, ' ', FALSE, TRUE)
    ) AS special_sedat_record
FROM {{ ref("int__special_sedat") }}
WHERE missing_street_name = FALSE
