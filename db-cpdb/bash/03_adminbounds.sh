#!/bin/bash
source bash/config.sh


echo
echo "#########################################"
echo "# create admin bounds relational tables #"
echo "#########################################"
echo

# attributes_maprojid_cd
echo 'Creating maprojid --> community district relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_cd.sql &

# attributes_maprojid_censustracts
echo 'Creating maprojid --> census tract, nta, boro relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_censustracts.sql &

# attributes_maprojid_congressionaldistricts
echo 'Creating maprojid --> congressional districts relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_congressionaldistricts.sql &

# attributes_maprojid_councildistrcts
echo 'Creating maprojid --> council distrcts relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_councildistricts.sql

wait
# attributes_maprojid_firecompanies
echo 'Creating maprojid --> fire companies relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_firecompanies.sql &

# attributes_maprojid_municipalcourtdistricts
echo 'Creating maprojid --> municipal court districts relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_municipalcourtdistricts.sql &

# attributes_maprojid_policeprecincts
echo 'Creating maprojid --> police precincts relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_policeprecincts.sql & 

# attributes_maprojid_schooldistricts
echo 'Creating maprojid --> school districts relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_schooldistricts.sql 

wait
# attributes_maprojid_stateassemblydistricts
echo 'Creating maprojid --> state assembly districts relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_stateassemblydistricts.sql &

# attributes_maprojid_statesenatedistricts
echo 'Creating maprojid --> state senate districts relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_statesenatedistricts.sql &

# # attributes_maprojid_trafficanalysiszones
echo 'Creating maprojid --> traffic analysis zones relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_trafficanalysiszones.sql &

# attributes_maprojid_bin
echo 'Creating maprojid --> bin relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_bin.sql &

# attributes_maprojid_bbl
echo 'Creating maprojid --> bin relational table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_bbl.sql

wait
echo 'Creating final output table'
psql $BUILD_ENGINE -f sql/attributes_maprojid_after.sql
