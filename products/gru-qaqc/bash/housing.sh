#!/bin/bash
set -e
source bash/config.sh
parse_flags $@

# Data loading
if [[ $import_data -eq 1 ]]; then 
    import dcp_developments
fi

# Execute
if [[ $python_script -eq 1 ]]; then 
    poetry run python -m python.housing
fi

# Export file
if [[ $export_data -eq 1 ]]; then 
    mkdir -p output/housing && (
        cd output/housing
        CSV_export tbins_certf_occp
        CSV_export versions
    )
fi

# Upload File
if [[ $upload_data -eq 1 ]]; then 
    Upload housing latest
    Upload housing $DATE
fi