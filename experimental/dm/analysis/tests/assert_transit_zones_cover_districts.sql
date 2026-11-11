-- The zones don't overlap, so a district's shares should add up to its whole area. Warns rather than fails:
-- BX12 and three park joint interest areas are known to fall short (see dim_community_district_transit_zone).
{{ config(severity='warn') }}
SELECT
    borocd,
    sum(share_of_district) AS share_covered
FROM {{ ref('dim_community_district_transit_zone') }}
GROUP BY borocd
HAVING abs(sum(share_of_district) - 1) > 0.01
