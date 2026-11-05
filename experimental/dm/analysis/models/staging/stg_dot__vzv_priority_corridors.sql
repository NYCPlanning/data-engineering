SELECT geom
FROM {{ source('sources', 'vzv_priority_corridors') }}
