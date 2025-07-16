{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['dtm_id']},
    ]
) }}

WITH validzones AS (
    SELECT * FROM {{ ref('stg__dcp_zoningdistricts') }}
),

dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

zonedist_priority AS (
    SELECT * FROM {{ ref('zonedist_priority') }}
),

lotzoneper AS (
    SELECT
        p.dtm_id,
        bbl,
        n.zonedist,
        ST_Area(
            CASE
                WHEN ST_CoveredBy(p.geom, n.geom) THEN p.geom::geography
                ELSE ST_Multi(ST_Intersection(p.geom, n.geom))::geography
            END
        ) AS segbblgeom,
        ST_Area(
            CASE
                WHEN ST_CoveredBy(n.geom, p.geom) THEN n.geom::geography
                ELSE ST_Multi(ST_Intersection(n.geom, p.geom))::geography
            END
        ) AS segzonegeom,
        ST_Area(p.geom::geography) AS allbblgeom,
        ST_Area(n.geom::geography) AS allzonegeom
    FROM dtm AS p
    INNER JOIN validzones AS n
        ON ST_Intersects(p.geom, n.geom)
),

lotzoneper_grouped AS (
    SELECT
        dtm_id,
        bbl,
        zonedist,
        allbblgeom,
        sum(segbblgeom) AS segbblgeom,
        sum(segzonegeom) AS segzonegeom,
        sum(allzonegeom) AS allzonegeom
    FROM lotzoneper
    GROUP BY dtm_id, allbblgeom, zonedist, bbl
),

initial_rankings AS (
    SELECT
        dtm_id,
        bbl,
        zonedist,
        segbblgeom,
        allbblgeom,
        segzonegeom,
        allzonegeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        -- zone districts per lot ranked by percent of lot covered
        row_number() OVER (
            PARTITION BY dtm_id
            ORDER BY segbblgeom DESC
        ) AS lot_row_number,
        -- per zoning district type, rank by 
        --   1) if lot meets 10% coverage by zoning district threshold
        --   2) area of coverage
        -- This is to get cases where special zoning district may only be assigned to one lot. 
        -- 1) is to cover edge case where largest section of specific large (but common) zoning district that happens to have largest area on small fraction of huge lot
        row_number() OVER (
            PARTITION BY zonedist
            ORDER BY (segbblgeom / allbblgeom) * 100 < 10, segzonegeom DESC
        ) AS zonedist_row_number
    FROM lotzoneper_grouped
),

lotzoneperorder_init AS (
    SELECT
        dtm_id,
        bbl,
        zonedist,
        segbblgeom,
        allbblgeom,
        perbblgeom,
        segzonegeom,
        allzonegeom,
        perzonegeom,
        row_number() OVER (
            PARTITION BY dtm_id
            ORDER BY segbblgeom DESC
        ) AS row_number,
        -- identifying whether zone of rank n for a lot is within 0.01 sq m of zone of rank (n-1) for same lot for tie-breaking logic in next query
        CASE
            WHEN
                row_number() OVER (
                    PARTITION BY dtm_id
                    ORDER BY segbblgeom DESC
                ) = 1
                OR lag(segbblgeom, 1, segbblgeom) OVER (
                    PARTITION BY dtm_id
                    ORDER BY segbblgeom DESC
                ) - segbblgeom
                > 0.01
                THEN 1
            ELSE 0
        END AS group_start
    FROM initial_rankings
    WHERE
        lot_row_number = 1
        OR perbblgeom >= 10
        OR zonedist_row_number = 1
),

group_column_added AS (
    SELECT
        dtm_id,
        bbl,
        segbblgeom,
        segzonegeom,
        -- this is not summing by any sql grouping, but rather summing in a window function as the rows are iterated through
        -- output column ends up being a grouping of lot/zone pairings that are "tied" within some limit and should be reordered
        --     based on ranking in zonedist_priority
        zonedist,
        row_number,
        sum(group_start) OVER (
            PARTITION BY dtm_id
            ORDER BY row_number
        ) AS reorder_group
    FROM lotzoneperorder_init
),

reorder_groups AS (
    SELECT
        dtm_id,
        reorder_group,
        min(row_number) - 1 AS order_start, -- starting point for re-ordering in "new_order" CTE down below
        array_agg(zonedist) AS zonedist
    FROM group_column_added
    GROUP BY dtm_id, reorder_group
    HAVING count(*) > 1 -- Filter to actual groups and not just individual rows
),

rows_to_reorder AS (
    SELECT
        dtm_id,
        reorder_group,
        order_start,
        unnest(zonedist) AS zonedist
    FROM reorder_groups
),

new_order AS (
    SELECT
        g.dtm_id,
        g.zonedist,
        row_number() OVER (
            PARTITION BY dtm_id, reorder_group
            ORDER BY priority ASC
        ) + g.order_start AS row_number
    FROM rows_to_reorder AS g
    INNER JOIN zonedist_priority AS zdp ON g.zonedist = zdp.zonedist
),

lotzoneperorder AS (
    SELECT
        a.dtm_id,
        a.zonedist,
        coalesce(new.row_number, a.row_number) AS row_number
    FROM lotzoneperorder_init AS a
    LEFT JOIN new_order AS new ON a.dtm_id = new.dtm_id AND a.zonedist = new.zonedist
)

SELECT * FROM lotzoneperorder
