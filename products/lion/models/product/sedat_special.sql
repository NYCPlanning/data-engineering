{{ config(
   materialized = 'table'
) }}

SELECT
   CONCAT(
       format_lion_text(lionkey, 10, ' '),
       COALESCE(parity, ' '),
       format_lion_text(street_name, 32, ' ', FALSE, TRUE),
       COALESCE(side_of_street, ' '),
       format_lion_text(lowaddress, 7, ' '),
       format_lion_text(low_addr_suffix, 8, ' ', FALSE, TRUE),
       format_lion_text(highaddress, 7, ' '),
       format_lion_text(high_addr_suffix, 8, ' ', FALSE, TRUE),
       format_lion_text(election_district, 3, '0'),
       format_lion_text(assembly_district, 2, '0'),
       format_lion_text(b7sc, 8, ' ', FALSE, TRUE)
   ) AS special_sedat_record
FROM {{ ref("int__special_sedat") }}
WHERE missing_street_name = FALSE
