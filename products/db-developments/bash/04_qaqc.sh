#!/bin/bash
source bash/config.sh

display "Creating QAQC Table for init table"
run_sql_file sql/qaqc/qaqc_init.sql

display "Creating QAQC Table for units table"
run_sql_file sql/qaqc/qaqc_units.sql

display "Creating QAQC Table for status"
run_sql_file sql/qaqc/qaqc_status.sql -v CAPTURE_DATE_PREV=$CAPTURE_DATE_PREV

display "Creating QAQC Table for qaqc mid and geo"
run_sql_file sql/qaqc/qaqc_mid.sql
run_sql_file sql/qaqc/qaqc_geo.sql

display "Creating QAQC Table for qaqc final"
run_sql_file sql/qaqc/qaqc_final.sql

display "appending to historic table"
import_qaqc_historic
run_sql_file sql/qaqc/qaqc_historic.sql -v VERSION=$VERSION

display "Creating QAQC Table for QAQC Application"
run_sql_file sql/qaqc/qaqc_app_additions.sql
run_sql_file sql/qaqc/qaqc_app.sql
run_sql_file sql/qaqc/qaqc_field_distribution.sql -v CAPTURE_DATE_PREV=$CAPTURE_DATE_PREV
run_sql_file sql/qaqc/qaqc_quarter_check.sql -v VERSION_PREV=$VERSION_PREV