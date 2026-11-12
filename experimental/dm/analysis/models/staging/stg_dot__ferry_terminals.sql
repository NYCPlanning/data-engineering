SELECT
    siteid AS terminal_id,
    site AS terminal_name,
    -- the file says OGC:CRS84, the same longitude and latitude as EPSG:4326, which the NYC Ferry stops use
    ST_SetCRS(geometry, 'EPSG:4326') AS geom
FROM {{ source('sources', 'dot_ferryterminals') }}
