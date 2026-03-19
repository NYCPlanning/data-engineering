#!/bin/bash
source bash/config.sh

python3 -m python.geocode
python3 -m python.geo_qaqc
