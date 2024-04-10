#!/bin/bash
source ingest/data_library/config.sh

for year in {2010..2020}
do
	import_csv dcp_dot_trafficinjuries $year
done
