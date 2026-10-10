-- int__topology__ap_police, one NYPD layer ({layer}: {layer_table}.{layer_id}), run once per layer
CREATE OR REPLACE TABLE police_{layer} AS
WITH at_centroid AS (
    SELECT DISTINCT ON (a.atomicid)
        a.atomicid,
        p.{layer_id} AS value,
        p.globalid
    FROM police_aps AS a
    INNER JOIN {layer_table} AS p ON ST_Intersects(p.geom, a.centroid)
    WHERE a.centroid_inside
    ORDER BY a.atomicid, p.globalid
),

overlap AS (
    SELECT
        a.atomicid,
        p.{layer_id} AS value,
        p.globalid,
        CASE
            WHEN ST_Covers(p.geom, a.geom) THEN ST_Area(a.geom)
            ELSE ST_Area(ST_Intersection(a.geom, p.geom))
        END AS overlap_sqft
    FROM (SELECT * FROM police_aps WHERE NOT centroid_inside) AS a
    INNER JOIN {layer_table} AS p ON ST_Intersects(a.geom, p.geom)
),

by_value AS (
    SELECT
        atomicid,
        value,
        sum(overlap_sqft) AS value_sqft,
        first(globalid ORDER BY overlap_sqft DESC) AS globalid
    FROM overlap
    WHERE overlap_sqft > 0
    GROUP BY atomicid, value
),

majority AS (
    SELECT DISTINCT ON (atomicid)
        atomicid,
        value,
        globalid,
        value_sqft / nullif(sum(value_sqft) OVER (PARTITION BY atomicid), 0) AS share
    FROM by_value
    ORDER BY atomicid ASC, value_sqft DESC, value ASC
)

SELECT
    a.atomicid,
    coalesce(c.value, m.value) AS value,
    round(m.share, 4) AS share,
    coalesce(c.globalid, m.globalid) AS globalid
FROM police_aps AS a
LEFT JOIN at_centroid AS c ON a.atomicid = c.atomicid
LEFT JOIN majority AS m ON a.atomicid = m.atomicid;
