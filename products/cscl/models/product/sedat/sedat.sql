SELECT
    FORMAT_LION_TEXT(lionkey, 10, ' ') AS lionkey,
    COALESCE(parity, ' ') AS parity,
    FORMAT_LION_TEXT(street_name, 32, ' ', FALSE, TRUE) AS street_name,
    COALESCE(side_of_street, ' ') AS side_of_street,
    FORMAT_LION_TEXT(lowaddress, 7, ' ') AS lowaddress,
    FORMAT_LION_TEXT(low_addr_suffix, 8, ' ', FALSE, TRUE) AS low_addr_suffix,
    FORMAT_LION_TEXT(highaddress, 7, ' ') AS highaddress,
    FORMAT_LION_TEXT(high_addr_suffix, 8, ' ', FALSE, TRUE) AS high_addr_suffix,
    FORMAT_LION_TEXT(election_district, 3, '0', TRUE) AS election_district,
    FORMAT_LION_TEXT(assembly_district, 2, '0', TRUE) AS assembly_district,
    FORMAT_LION_TEXT(b7sc, 8, ' ') AS b7sc
FROM {{ ref("int__sedat") }}
