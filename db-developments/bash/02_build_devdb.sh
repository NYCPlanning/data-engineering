#!/bin/bash
source bash/config.sh

display "Starting to build Developments DB"
psql $BUILD_ENGINE -f sql/_function.sql
psql $BUILD_ENGINE -f sql/_procedures.sql
psql $BUILD_ENGINE -f sql/bis/_init.sql
psql $BUILD_ENGINE -f sql/now/_init.sql
psql $BUILD_ENGINE -f sql/_init.sql
count _INIT_devdb

display "Assign geoms to _GEO_devdb and create GEO_devdb"
psql $BUILD_ENGINE -f sql/_geo.sql
psql $BUILD_ENGINE -f sql/_geo_corrections.sql
count GEO_devdb

display "Fill NULLs spatial boundries in GEO_devdb through spatial joins. 
  This is the consolidated spatial attributes table"
psql $BUILD_ENGINE -f sql/_spatial.sql
count SPATIAL_devdb
psql $BUILD_ENGINE -f sql/init.sql
count INIT_devdb

display "Adding on PLUTO columns"
psql $BUILD_ENGINE -f sql/_pluto.sql
count PLUTO_devdb

display "Create CO fields: 
      effectivedate, 
      date_complete
      year_complete, 
      co_latest_effectivedate, 
      co_latest_units, 
      co_latest_certtype"
psql $BUILD_ENGINE -f sql/_co.sql
count CO_devdb

display "Creating OCC fields: 
      occ_initial, 
      occ_proposed"
psql $BUILD_ENGINE -f sql/_occ.sql
count OCC_devdb

display "Creating UNITS fields: 
      classa_init,
      classa_prop,
      hotel_init,
      hotel_prop,
      otherb_init,
      otherb_prop,
      classa_net,
      resid_flag"
psql $BUILD_ENGINE -f sql/_units.sql
count UNITS_devdb

display "Creating status_q fields: date_permittd,
      permit_year,
      permit_qrtr,
      _complete_year,
      _complete_qrtr"
psql $BUILD_ENGINE -f sql/_status_q.sql
count STATUS_Q_devdb

display "Combining INIT_devdb with OCC_devdb, 
      PLUTO_devdb, 
      CO_devdb, 
      OCC_devdb, 
      UNITS_devdb,
      STATUS_Q_devdb to create _MID_devdb"
psql $BUILD_ENGINE -f sql/_mid.sql
count _MID_devdb

display "Creating status fields: 
      job_status,
      date_lastupdt,
      date_permittd,
      job_inactive"

psql $BUILD_ENGINE\
  -v CAPTURE_DATE=$CAPTURE_DATE\
  -f sql/_status.sql

display "Combining _MID_devdb with STATUS_devdb to create MID_devdb,
            Creating nonres_flag field"
psql $BUILD_ENGINE -f sql/mid.sql
count MID_devdb

display "Creating HNY fields: 
      hny_id,
      classa_hnyaff,
      all_hny_units,
      hny_jobrelate"
psql $BUILD_ENGINE -f sql/_hny_match.sql
psql $BUILD_ENGINE -f sql/_hny_join.sql
count HNY_devdb_lookup

display "Creating FINAL_devdb and formatted QAQC table"
psql $BUILD_ENGINE -v VERSION=$VERSION  -f sql/final.sql
psql $BUILD_ENGINE -f sql/corrections.sql
