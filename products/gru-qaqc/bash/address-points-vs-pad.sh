#!/bin/bash
set -e
source bash/config.sh
parse_flags $@

# Data loading
if [[ $import_data -eq 1 ]]; then 
    import dcp_addresspoints $(get_geosupport_version)
fi

# Execute
if [[ $python_script -eq 1 ]]; then 
    poetry run python -m python.address-points-vs-pad
fi

# Export file
if [[ $export_data -eq 1 ]]; then 
    mkdir -p output/address-points-vs-pad && (
        cd output/address-points-vs-pad
        CSV_export rejects_pad_addrpts
        CSV_export versions
    )
fi

# Upload File
if [[ $upload_data -eq 1 ]]; then 
    Upload address-points-vs-pad latest
    Upload address-points-vs-pad $DATE
fi