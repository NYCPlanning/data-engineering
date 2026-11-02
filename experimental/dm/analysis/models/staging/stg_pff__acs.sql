-- Only the census tract, CDTA, and citywide rows. The citywide row with every variable is labeled
-- City2010, even in this file's 2020-2024 estimates; City2020 has only a subset.
SELECT
    CASE labs_geotype WHEN 'CT2020' THEN 'census_tract' WHEN 'CDTA2020' THEN 'cdta' WHEN 'City2010' THEN 'city' END AS geography,
    labs_geoid AS geoid,
    pff_variable,
    e AS estimate,
    m AS margin_of_error,
    c AS coefficient_of_variation
FROM {{ source('sources', 'pff_acs_2024') }}
WHERE labs_geotype IN ('CT2020', 'CDTA2020', 'City2010')
