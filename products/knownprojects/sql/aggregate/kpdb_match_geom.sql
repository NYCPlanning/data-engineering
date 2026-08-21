{{ config(
    materialized='table',
    tags=['aggregate_general'],
    post_hook="CREATE INDEX IF NOT EXISTS {{ this.identifier }}_gix ON {{ this }} USING gist (match_geom)"
) }}

{{ match_geometry(source_model='kpdb_deduplicated') }}
