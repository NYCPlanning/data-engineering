#!/bin/bash
set -e
source bash/config.sh
parse_flags $@

# Data loading
if [[ $import_data -eq 1 ]]; then 
    import dcp_dcmstreetcenterline $(get_geosupport_version)
fi

# Execute
if [[ $python_script -eq 1 ]]; then 
    poetry run python -m python.dcm-streetname
fi

# Export file
if [[ $export_data -eq 1 ]]; then 
    mkdir -p output/dcm-streetname && (
        cd output/dcm-streetname
        CSV_export rejects_sn_dcm_snd
        CSV_export versions
    )
fi

# Upload File
if [[ $upload_data -eq 1 ]]; then 
    Upload dcm-streetname latest
    Upload dcm-streetname $DATE
fi