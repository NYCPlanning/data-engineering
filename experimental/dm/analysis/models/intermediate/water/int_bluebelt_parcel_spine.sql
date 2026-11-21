-- DEP-owned Bluebelt land only, leaving out parcels DEP planned to buy and DEP land the file marks Not Bluebelt.
-- Each parcel lists the BMPs that fall inside it.
WITH parcels AS (
    SELECT
        row_number() OVER () AS parcel_number,
        *
    FROM {{ ref('stg_dcp__bluebelt_properties') }}
    WHERE property_type IN ('DEP Parcel', 'DEP_Owned_Roadbeds_and_Partials')
)

SELECT
    parcels.watershed,
    parcels.property_type,
    parcels.block,
    parcels.lot,
    string_agg(
        bmp.bmp_type || ' (' || coalesce('built ' || bmp.year_built, lower(bmp.status)) || ')',
        ', ' ORDER BY bmp.bmp_id
    ) AS bmps,
    any_value(parcels.geom) AS geom
FROM parcels
LEFT JOIN {{ ref('int_bluebelt_bmp_spine') }} AS bmp
    ON ST_Intersects(parcels.geom, bmp.geom)
GROUP BY parcels.parcel_number, parcels.watershed, parcels.property_type, parcels.block, parcels.lot
