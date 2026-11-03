-- NTAs that touch, even at a corner. EPSG:2263 is in feet. always_xy because the source is lon/lat.
SELECT
    nta.nta2020,
    neighbor.nta2020 AS neighbor_nta2020,
    ST_Length(
        ST_Transform(ST_Intersection(nta.geom, neighbor.geom), 'EPSG:4326', 'EPSG:2263', true)
    ) AS shared_boundary_feet
FROM {{ ref('int_nta_spine') }} AS nta
INNER JOIN {{ ref('int_nta_spine') }} AS neighbor
    ON nta.nta2020 != neighbor.nta2020 AND ST_Intersects(nta.geom, neighbor.geom)
