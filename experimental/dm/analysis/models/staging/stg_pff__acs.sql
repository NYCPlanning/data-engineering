-- Only the CDTA rows, the closest ACS geography to community districts.
SELECT
    labs_geoid AS cdta2020,
    pff_variable,
    domain,
    e::double AS estimate,
    m::double AS margin_of_error,
    c::double AS coefficient_of_variation,
    p::double AS percent,
    z::double AS percent_margin_of_error
FROM {{ source('sources', 'pff_acs_2024') }}
WHERE labs_geotype = 'CDTA2020'
