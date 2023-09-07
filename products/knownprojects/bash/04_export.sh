#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# Upload
SENDER=${1:-unknown}
python3 -m python.upload $SENDER
