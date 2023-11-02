#!/bin/bash
source ./ae_output_scripts/config.sh
set_error_traps

TILESET="${TILE_SET//_}"
conf="data/mvt_${TILESET}_conf.json"
rendered_conf=data/conf.json

cat $conf \
    | jq --arg minzoom "$MIN_ZOOM" --arg maxzoom "$MAX_ZOOM" --arg schema "$BUILD_ENGINE_SCHEMA" \
    'with_entries(.key |= sub("SCHEMA";$schema)) | map_values(.minzoom |= $minzoom | .maxzoom |= $maxzoom)' \
    > $rendered_conf

mkdir -p ae_output && (
    cd ae_output

    echo "Generating output MVT files"
    mkdir -p tilesets && (
        cd tilesets
        mvt_export ${TILE_SET} ../../$rendered_conf \
            $BUILD_ENGINE_SCHEMA.ae_tileset_${TILESET}_fill $BUILD_ENGINE_SCHEMA.ae_tileset_${TILESET}_label \
            -dsco MINZOOM=$MIN_ZOOM -dsco MAXZOOM=$MAX_ZOOM
    )
)
