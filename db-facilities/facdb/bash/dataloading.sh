#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")

source $FILE_DIR/../../../bash/utils.sh
set_error_traps
max_bg_procs 5
VERSION_PREV=$1

create_source_data_table

import_recipe dcp_facilities_with_unmapped $VERSION_PREV &
import_recipe dcp_mappluto_wi &
import_recipe dcp_boroboundaries_wi &
import_recipe dcp_ct2010 &
import_recipe dcp_ct2020 &
import_recipe dcp_councildistricts 22c &
import_recipe dcp_cdboundaries &
import_recipe dcp_nta2010 &
import_recipe dcp_nta2020 &
import_recipe dcp_policeprecincts &
import_recipe doitt_zipcodeboundaries &
import_recipe doitt_buildingfootprints &
import_recipe dcp_school_districts &

wait
