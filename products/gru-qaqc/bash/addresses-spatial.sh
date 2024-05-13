#!/bin/bash
set -e
source bash/config.sh
parse_flags $@

# Data loading
if [[ $import_data -eq 1 ]]; then 
    import dcp_addresspoints $(get_geosupport_version) &
    import dcp_atomicpolygons $(get_geosupport_version)
    wait
fi

# Execute
if [[ $python_script -eq 1 ]]; then 
    poetry run python -m python.addresses-spatial
fi

# Export file
if [[ $export_data -eq 1 ]]; then 
    mkdir -p output/addresses-spatial && (
        cd output/addresses-spatial
        rm -f addresses-spatial.zip
        CSV_export geocode_diffs_address_spatial
        CSV_export rejects_address_spatial
        CSV_export versions
    )
fi

# Upload File
if [[ $upload_data -eq 1 ]]; then 
    Upload addresses-spatial latest
    Upload addresses-spatial $DATE
fi
