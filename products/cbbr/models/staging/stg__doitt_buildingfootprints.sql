select
    *,
    wkb_geometry as geom
from {{ source('recipe_sources', 'doitt_buildingfootprints') }}
