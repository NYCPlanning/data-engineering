{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Appendix D carries no schema for this layer; per ETL spec ch. 10 the district
-- identifier is the only required attribute.
--
-- Prod's nyura is a stale copy of nybid's schema (BIDID/BID/BOROUGH) rather than
-- anything urban-renewal related. Both prod and the CSCL source layer are empty, so
-- the mistake is invisible in the data. The real URA identifiers are emitted here
-- instead of reproducing that; expect a structural diff on this one layer.

SELECT
    d.uraid::int AS "URAID",
    d.uranum::int AS "URANum",
    d.site_name AS "SiteName",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__urbanrenewalarea') }} AS d
