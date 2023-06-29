#!/bin/bash
source ../bash/utils.sh
set_env ../.env

create_source_data_table

import_recipe dcp_pluto &
import_recipe dcp_colp &
import_recipe dcas_ipis &
import_recipe dof_air_rights_lots &
import_recipe dcp_boroboundaries_wi &
wait 
echo "dataloading complete"