SELECT * FROM {{ ref('int_cd_transit_zone_spine') }}
ORDER BY borocd, transit_zone
