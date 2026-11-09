SELECT count(*) AS n_ahft_districts
FROM {{ ref('dim_community_district') }}
WHERE is_ahft_district
HAVING count(*) != 12
