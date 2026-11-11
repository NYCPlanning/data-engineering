SELECT * FROM {{ ref('int_bluebelt_parcel_spine') }}
ORDER BY watershed, block, lot
