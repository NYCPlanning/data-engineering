WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "usfws_nyc_wetlands"), left_by="geometry") }}
)

--- wetland_types
--Other
--Freshwater Pond
--Estuarine and Marine Deepwater
--Riverine
--Estuarine and Marine Wetland
--Lake
--Freshwater Forested/Shrub Wetland
---
SELECT
    'usfws_wetlands' AS variable_type,
    wetland_type AS variable_id,
    geom AS raw_geom,
    NULL AS buffer
FROM clipped_to_nyc
