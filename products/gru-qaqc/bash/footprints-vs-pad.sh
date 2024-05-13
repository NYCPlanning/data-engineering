#!/bin/bash
set -e
source bash/config.sh
parse_flags $@

# Data loading
if [[ $import_data -eq 1 ]]; then 
    import doitt_buildingfootprints
fi

# Execute
if [[ $python_script -eq 1 ]]; then 
    poetry run python -m python.footprints-vs-pad
fi

# Export file
if [[ $export_data -eq 1 ]]; then 
    mkdir -p output/footprints-vs-pad && (
        cd output/footprints-vs-pad
        rm -f footprints-vs-pad.zip
        CSV_export rejects_footprintbin_padbin
        CSV_export versions
    )
fi

# Upload File
if [[ $upload_data -eq 1 ]]; then 
    Upload footprints-vs-pad latest
    Upload footprints-vs-pad $DATE
fi
