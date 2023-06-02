#!/bin/bash
CURRENT_DIR=$(dirname "$(readlink -f "$0")")
source $CURRENT_DIR/config.sh
max_bg_procs 5

import_public dcp_facilities_with_unmapped $VERSION_PREV &
import_public dcp_mappluto_wi &
import_public dcp_boroboundaries_wi &
import_public dcp_ct2010 &
import_public dcp_ct2020 &
import_public dcp_councildistricts 22c &
import_public dcp_cdboundaries &
import_public dcp_nta2010 &
import_public dcp_nta2020 &
import_public dcp_policeprecincts &
import_public doitt_zipcodeboundaries &
import_public doitt_buildingfootprints &
import_public dcp_school_districts &

wait
