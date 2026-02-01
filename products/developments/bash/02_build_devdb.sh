#!/bin/bash
source bash/config.sh

dbt deps
dbt debug

run_sql_file sql/_create.sql

display "Starting to build Developments DB"
run_sql_file sql/_function.sql
run_sql_file sql/_procedures.sql -v build_schema=${BUILD_ENGINE_SCHEMA}
run_sql_file sql/_bis.sql
run_sql_file sql/_now.sql
run_sql_file sql/_hpd.sql
run_sql_file sql/_init.sql -v build_schema=${BUILD_ENGINE_SCHEMA}
sql_table_summary _INIT_devdb

display "Assign geoms to _GEO_devdb and create GEO_devdb"
python3 python/geocode_dob.py
run_sql_file sql/_geo.sql
run_sql_file sql/_geo_corrections.sql
sql_table_summary GEO_devdb

display "Fill NULLs spatial boundries in GEO_devdb through spatial joins. This is the consolidated spatial attributes table"
run_sql_file sql/_spatial.sql
sql_table_summary SPATIAL_devdb
run_sql_file sql/init.sql
sql_table_summary INIT_devdb

display "Adding on PLUTO columns"
run_sql_file sql/_pluto.sql
sql_table_summary PLUTO_devdb

display "Create CO fields: 
      effectivedate, 
      date_complete
      year_complete, 
      co_latest_effectivedate, 
      co_latest_units, 
      co_latest_certtype"
run_sql_file sql/_co.sql
sql_table_summary CO_devdb

display "Creating OCC fields: 
      occ_initial, 
      occ_proposed"
run_sql_file sql/_occ.sql -v build_schema=${BUILD_ENGINE_SCHEMA}
sql_table_summary OCC_devdb

display "Creating UNITS fields: 
      classa_init,
      classa_prop,
      hotel_init,
      hotel_prop,
      otherb_init,
      otherb_prop,
      classa_net,
      resid_flag"
run_sql_file sql/_units.sql -v build_schema=${BUILD_ENGINE_SCHEMA}
sql_table_summary UNITS_devdb

display "Creating status_q fields: date_permittd,
      permit_year,
      permit_qrtr,
      _complete_year,
      _complete_qrtr"
run_sql_file sql/_status_q.sql
sql_table_summary STATUS_Q_devdb

display "Combining INIT_devdb with OCC_devdb, 
      PLUTO_devdb, 
      CO_devdb, 
      OCC_devdb, 
      UNITS_devdb,
      STATUS_Q_devdb to create _MID_devdb"
run_sql_file sql/_mid.sql -v build_schema=${BUILD_ENGINE_SCHEMA}
sql_table_summary _MID_devdb

display "Creating status fields: 
      job_status,
      date_lastupdt,
      date_permittd,
      job_inactive"

run_sql_file sql/_status.sql -v CAPTURE_DATE=$CAPTURE_DATE -v build_schema=${BUILD_ENGINE_SCHEMA}

display "Combining _MID_devdb with STATUS_devdb to create MID_devdb,
            Creating nonres_flag field"
run_sql_file sql/mid.sql
sql_table_summary MID_devdb

display "Creating HNY fields: 
      hny_id,
      classa_hnyaff,
      all_hny_units,
      hny_jobrelate"
run_sql_file sql/_hny_union.sql
run_sql_file sql/_hny_match.sql
run_sql_file sql/_hny_join.sql
sql_table_summary HNY_devdb_lookup

display "Creating FINAL_devdb and formatted QAQC table"
run_sql_file sql/final.sql -v VERSION=$VERSION
run_sql_file sql/corrections.sql
