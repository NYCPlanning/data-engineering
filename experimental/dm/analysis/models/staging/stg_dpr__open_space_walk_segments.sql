SELECT
    "SegmentID" AS segment_id,
    "CommunityBoard"::INTEGER AS borocd,
    population,
    "travel_time:Walk" AS walk_minutes_to_open_space,
    ST_Transform(geom, 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'dpr_open_space_walk_segments') }}
