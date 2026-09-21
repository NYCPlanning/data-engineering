{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['atomicid']},
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- Reassembles each atomic polygon from its node-snapped rings (int__atomicpolygon_rings):
-- exterior ring + any interior/hole rings per part, unioned back into a multipolygon per
-- atomicid. This is the POC's payoff - geometries that share a canonical node at a
-- boundary now literally touch there, not just sit close to it. Compare against
-- stg__atomicpolygons.geom, which is untouched by any of this.
WITH parts AS (
    SELECT
        atomicid,
        part_index,
        st_makepolygon(
            (array_agg(ring_geom ORDER BY ring_index) FILTER (WHERE ring_index = 1))[1],
            coalesce(array_agg(ring_geom ORDER BY ring_index) FILTER (WHERE ring_index > 1), ARRAY[]::geometry[])
        ) AS part_geom
    FROM {{ ref('int__atomicpolygon_rings') }}
    GROUP BY atomicid, part_index
)

SELECT
    atomicid,
    st_makevalid(st_multi(st_collect(part_geom ORDER BY part_index))) AS geom
FROM parts
GROUP BY atomicid
