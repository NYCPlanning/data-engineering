SELECT * FROM {{ ref('int_transit_zone_spine') }}
ORDER BY transit_zone
