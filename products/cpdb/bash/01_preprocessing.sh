#!/bin/bash
source ../../bash/utils.sh
set_error_traps

echo "fixing dot_bridges"
python3 python/dot_bridges.py
