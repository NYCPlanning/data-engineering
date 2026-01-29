#!/bin/bash
# Define variables ----------------------------------------

# data dir expects two file geodatabases - $GDB and $BACKUP_GDB, defined below
DATA_DIR=$1

GDB="test_db.gdb"
BACKUP_GDB="_backup__${GDB}"
DRIVER="OpenFileGDB"
PROD_LAYER="test"
TEMP_LAYER="intermediate_data"
SHP="test"
METADATA=$(cat <<EOF
<metadata xml:lang='en'>
    <Esri>
        <Title>Some nonsense</Title>
        <Time>$(date '+%Y%m%d_%H%M%S')</Time>
    </Esri>
</metadata>
EOF
)

# Prep data directory -------------------------------------
pushd $DATA_DIR
echo "--> Data directory: $DATA_DIR"
echo -e "--> Data dir contents: \\n$(ls $DATA_DIR)"

echo "--> Deleting: $GDB"
rm -r "$GDB"
echo "--> Restoring: $GDB from $BACKUP_GDB"
cp -r "$BACKUP_GDB" "$GDB"

# Write metadata ------------------------------------------
echo "--> Writing metadata to ${GDB}"
# ogr2ogr -f "OpenFileGDB" "$GDB" "$SHP" -lco DOCUMENTATION="$METADATA"     # pre-GDAL CLI method

# ...create intermediate dataset...
echo "--> Creating an intermediate layer"
gdal vector edit \
    --input-format "$DRIVER" \
    --input "$GDB" \
    --output "$GDB" \
    --input-layer "$PROD_LAYER" \
    --output-layer "$TEMP_LAYER" \
    --update

# ...add metadata...
echo "--> Writing metadata to layer"
gdal vector edit \
    --input-format "$DRIVER" \
    --input "$GDB" \
    --output "$GDB" \
    --input-layer "$TEMP_LAYER" \
    --output-layer "$PROD_LAYER" \
    --layer-creation-option DOCUMENTATION="$METADATA" \
    --overwrite-layer

# ...delete intermediate layer...
echo "--> Deleting temp layer '${TEMP_LAYER}':"
gdal vector sql \
    --input-format "$DRIVER" \
    --input "$GDB" \
    --sql "DROP TABLE ${TEMP_LAYER}" \
    --update

# ...return dataset info...
echo "--> Getting layer list for '${GDB}':"
gdal vector info --format=JSON "${GDB}" | \
    jq -r '.rootGroup.layerNames'

# ...return metadata...
echo "--> Getting metadata for layer '${PROD_LAYER}':"
gdal vector info $GDB \
    --sql "GetLayerMetadata ${PROD_LAYER}" \
    --features --format=JSON | \
        jq -r '.layers[0].features[0].properties.FIELD_1' | \
        xmllint --format -


popd

# this pipeline isnt't working, but retaining as an example
# gdal vector pipeline \
#     ! read "$GDB" \
#     ! edit \
#         --input-format "$DRIVER" \
#         --input "$GDB" \
#         --output "$GDB" \
#         --input-layer "$PROD_LAYER" \
#         --output-layer "$TEMP_LAYER" \
#         --update \
#     ! edit \
#         --input-format "$DRIVER" \
#         --input "$GDB" \
#         --output "$GDB" \
#         --input-layer "$TEMP_LAYER" \
#         --output-layer "$PROD_LAYER" \
#         --layer-creation-option DOCUMENTATION="$METADATA" \
#         --overwrite-layer \
#     ! sql \
#         --input-format "$DRIVER" \
#         --input "$GDB" \
#         --sql "DROP TABLE ${TEMP_LAYER}" \
#         --update