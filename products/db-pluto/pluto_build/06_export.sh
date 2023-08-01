#!/bin/bash
source ./bash/config.sh
set_error_traps

echo "Vacuuming build DB"
# NOTE in the future may need to drop big tables (dof_dtm, pluto_input_cama_dof, pluto_input_geocodes)
run_sql_command "VACUUM (FULL, ANALYZE, VERBOSE)"

mkdir -p output
cd output
    
echo "version: ${VERSION}" > version.txt
echo "date: ${DATE}" >> version.txt
# csv_export ${BUILD-ENGINE} pluto_changes
csv_export pluto_removed_records
csv_export pluto_changes_not_applied
csv_export pluto_changes_applied
zip pluto_changes.zip *
ls | grep -v pluto_changes.zip | xargs rm


csv_export source_data_versions

echo "Exporting gdbs and shapefiles"

# DEV section start: low disk space

# mappluto.gdb
fgdb_export_pluto mappluto_gdb &

# mappluto_unclipped.gdb
fgdb_export_pluto mappluto_unclipped_gdb &

# mappluto
shp_export_pluto mappluto MULTIPOLYGON &

# mappluto_unclipped
shp_export_pluto mappluto_unclipped MULTIPOLYGON &

# DEV section end: low disk space

wait
echo "Exporting pluto csv"

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

echo "Exporting DOF"
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

echo "Exporting QAQC"
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

wait 
cd ..

# "standard" export to branch folder
upload "db-pluto" "${DATE}" &
upload "db-pluto" "latest" &
upload "db-pluto" "${DATE}" "${VERSION}" &
upload "db-pluto" "latest" "${VERSION}"
