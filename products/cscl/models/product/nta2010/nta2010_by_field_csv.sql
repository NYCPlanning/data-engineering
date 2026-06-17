WITH source AS (
    SELECT
        borough,
        censustract::INTEGER AS censustract_int,
        neighborhood_code,
        neighborhood_name,
        puma::INTEGER AS puma_int
    FROM {{ ref('stg__neighborhoodpumacodes') }}
),
preprocessed AS (
    SELECT
        borough,
        -- Census tracts stored as integers where last 2 digits are decimal suffix
        -- Convert to BBBBSS format: 4-byte basic (RJSF) + 2-byte suffix (RJZF or blank)
        LPAD((censustract_int / 100)::TEXT, 4, ' ')
        || CASE
            WHEN censustract_int % 100 = 0 THEN '  '  -- No suffix: 2 blanks
            ELSE LPAD((censustract_int % 100)::TEXT, 2, '0')  -- Suffix: 2-digit zero-filled
        END AS censustract,
        neighborhood_code,
        neighborhood_name,
        puma_int
    FROM source
)
SELECT
    borough AS "BOROUGH",
    censustract AS "CENSUSTRACT",
    neighborhood_code AS "NEIGHBORHOOD_CODE",
    RPAD(neighborhood_name, 75, ' ') AS "NEIGHBORHOOD_NAME",  -- Pad to 75 chars with spaces
    LPAD(puma_int::TEXT, 5, '0') AS "PUMA"  -- Zero-pad to 5 chars
FROM preprocessed
ORDER BY borough, censustract
