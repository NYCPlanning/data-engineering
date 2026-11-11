-- The area a district's map shows, so layers can be clipped to it rather than to the district and maps don't stop
-- at the district line. The district's bounding box, widened to at least 2.5:1 for the notebook's wide maps, plus
-- 10%. Measured in feet (EPSG:2263) so the ratio holds. always_xy because the source is lon/lat.
{%- set aspect = 2.5 %}
{%- set margin = 1.1 %}
WITH boxes AS (
    SELECT
        borocd,
        ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true) AS geom
    FROM {{ ref('int_cd_spine') }}
),

sizes AS (
    SELECT
        borocd,
        (ST_XMin(geom) + ST_XMax(geom)) / 2 AS center_x,
        (ST_YMin(geom) + ST_YMax(geom)) / 2 AS center_y,
        greatest(ST_XMax(geom) - ST_XMin(geom), (ST_YMax(geom) - ST_YMin(geom)) * {{ aspect }})
        / 2 * {{ margin }} AS half_width,
        greatest(ST_YMax(geom) - ST_YMin(geom), (ST_XMax(geom) - ST_XMin(geom)) / {{ aspect }})
        / 2 * {{ margin }} AS half_height
    FROM boxes
)

SELECT
    borocd,
    ST_Transform(
        ST_MakeEnvelope(center_x - half_width, center_y - half_height, center_x + half_width, center_y + half_height),
        'EPSG:2263',
        'EPSG:4326',
        true
    ) AS frame
FROM sizes
