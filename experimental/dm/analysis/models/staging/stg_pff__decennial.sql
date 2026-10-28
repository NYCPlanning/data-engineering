-- Only the community district rows (3-digit geoids, which are borocd). The files also carry
-- city, borough, CDTA, council district, NTA, and tract rows.
WITH unioned AS (
    SELECT * FROM {{ source('sources', 'pff_decennial_2010') }}
    UNION ALL
    SELECT * FROM {{ source('sources', 'pff_decennial_2020') }}
)

SELECT
    year::integer AS census_year,
    geoid::integer AS borocd,
    variable AS pff_variable,
    value
FROM unioned
WHERE length(geoid) = 3
