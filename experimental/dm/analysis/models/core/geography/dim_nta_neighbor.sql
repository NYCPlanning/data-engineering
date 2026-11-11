SELECT * FROM {{ ref('int_nta_neighbor_spine') }}
ORDER BY nta2020, neighbor_nta2020
