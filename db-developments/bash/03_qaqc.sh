#!/bin/bash
source bash/config.sh

display "Creating QAQC Table for init table"
psql $BUILD_ENGINE -f sql/qaqc/qaqc_init.sql

display "Creating QAQC Table for units table"
psql $BUILD_ENGINE -f sql/qaqc/qaqc_units.sql

display "Creating QAQC Table for status"
psql $BUILD_ENGINE\
  -v CAPTURE_DATE_PREV=$CAPTURE_DATE_PREV\
  -f sql/qaqc/qaqc_status.sql

display "Creating QAQC Table for qaqc mid and geo"
psql $BUILD_ENGINE -f sql/qaqc/qaqc_mid.sql
psql $BUILD_ENGINE -f sql/qaqc/qaqc_geo.sql

display "Creating QAQC Table for qaqc final"
psql $BUILD_ENGINE -f sql/qaqc/qaqc_final.sql

display "appending to historic table"
import_qaqc_historic
psql $BUILD_ENGINE -v VERSION=$VERSION -f sql/qaqc/qaqc_historic.sql 

display "Creating QAQC Table for QAQC Application"
psql $BUILD_ENGINE -f sql/qaqc/qaqc_app_additions.sql
psql $BUILD_ENGINE -f sql/qaqc/qaqc_app.sql
psql $BUILD_ENGINE\
   -v CAPTURE_DATE_PREV=$CAPTURE_DATE_PREV\
   -f sql/qaqc/qaqc_field_distribution.sql
psql $BUILD_ENGINE\
  -v VERSION_PREV=$VERSION_PREV\
  -f sql/qaqc/qaqc_quarter_check.sql