#!/bin/bash
source bin/config.sh

psql $BUILD_ENGINE -f sql/corr_create.sql

echo "Applying corrections to PLUTO"
psql $BUILD_ENGINE -f sql/corr_lotarea.sql
psql $BUILD_ENGINE -f sql/corr_yearbuilt_lpc.sql
psql $BUILD_ENGINE -f sql/corr_ownername_city.sql
psql $BUILD_ENGINE -f sql/corr_ownername_punctuation.sql
psql $BUILD_ENGINE -f sql/corr_communitydistrict.sql
psql $BUILD_ENGINE -f sql/corr_numfloors.sql
psql $BUILD_ENGINE -f sql/corr_units.sql
psql $BUILD_ENGINE -f sql/corr_inwoodrezoning.sql
psql $BUILD_ENGINE -f sql/corr_dropoldrecords.sql
psql $BUILD_ENGINE -f sql/remove_unitlots.sql

exit 0