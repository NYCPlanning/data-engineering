#!/bin/bash
source ./ae_output_scripts/config.sh
set_error_traps

python3 -m dcpy.utils.s3 \
    --bucket de-sandbox \
    --folder-path ae_output \
    --s3-path ae-pilot-project \
    --acl public-read \
    --max-files 20000000 \
    --contents-only \
    --keep-existing
