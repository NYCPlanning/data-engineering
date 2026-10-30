-- Only the community district (3-digit geoid, the borocd) and census tract (7-digit geoid, the
-- boroct2020) rows. The files also carry city, borough, CDTA, council district, NTA, and block rows.
WITH unioned AS (
    SELECT * FROM {{ source('sources', 'pff_decennial_2010') }}
    UNION ALL
    SELECT * FROM {{ source('sources', 'pff_decennial_2020') }}
)

SELECT
    year::integer AS census_year,
    CASE length(geoid) WHEN 3 THEN 'community_district' WHEN 7 THEN 'census_tract' END AS geography,
    geoid,
    variable AS pff_variable,
    value
FROM unioned
WHERE length(geoid) IN (3, 7)
