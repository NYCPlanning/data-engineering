#!/bin/bash
source ../../bash_utils/config.sh
set_env ../../.env
set_env ./version.env

mkdir -p output
cd output
    
echo "version: ${VERSION}" > version.txt
echo "date: ${DATE}" >> version.txt
# csv_export ${BUILD-ENGINE} pluto_changes
csv_export ${BUILD-ENGINE} pluto_removed_records
csv_export ${BUILD-ENGINE} pluto_changes_not_applied
csv_export ${BUILD-ENGINE} pluto_changes_applied
zip pluto_changes.zip *
ls | grep -v pluto_changes.zip | xargs rm


csv_export ${BUILD_ENGINE} source_data_versions

# mappluto.gdb
fgdb_export mappluto_gdb MULTIPOLYGON &

# mappluto_unclipped.gdb
fgdb_export mappluto_unclipped_gdb MULTIPOLYGON &

# mappluto
shp_export mappluto MULTIPOLYGON &

# mappluto_unclipped
shp_export mappluto_unclipped MULTIPOLYGON &

# Pluto
mkdir -p pluto &&
  (cd pluto
    rm -f pluto.zip
    run_sql_command "\COPY ( 
          SELECT * FROM export_pluto
        ) TO STDOUT DELIMITER ',' CSV HEADER;" > pluto.csv
    echo "${VERSION}" > version.txt
    echo "$(wc -l pluto.csv)" >> version.txt
    zip pluto.zip *
    ls | grep -v pluto.zip | xargs rm
  )

# BBL and Council info for DOF
mkdir -p dof && 
  (cd dof
    rm -f bbl_council.zip
    run_sql_command "\COPY ( 
          SELECT bbl, council FROM export_pluto
          WHERE bbl is not null
        ) TO STDOUT DELIMITER ',' CSV HEADER;" > bbl_council.csv
    echo "${VERSION}" > version.txt
    zip bbl_council.zip *
    ls | grep -v bbl_council.zip | xargs rm
  )

mkdir -p qaqc && 
  (cd qaqc
    for table in qaqc_aggregate qaqc_expected qaqc_mismatch qaqc_null qaqc_outlier
    do
      run_sql_command "\COPY ( 
          SELECT * FROM ${table}
        ) TO STDOUT DELIMITER ',' CSV HEADER;" > ${table}.csv
      pg_dump -d ${BUILD_ENGINE} -t ${table} -f ${table}.sql  
    done

  )

cd ..

wait
upload "db-pluto" "${VERSION}/${DATE}" &
upload "db-pluto" "${branchname}/${DATE}"
