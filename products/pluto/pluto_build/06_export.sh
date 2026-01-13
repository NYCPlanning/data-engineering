#!/bin/bash
source ./bash/config.sh
set_error_traps

mkdir -p output

cd output

csv_export pluto_removed_records
csv_export pluto_changes_not_applied
csv_export pluto_changes_applied
zip pluto_changes.zip *
ls | grep -v pluto_changes.zip | xargs rm

cp ../../source_data_versions.csv ./
cp ../../build_metadata.json ./

echo "${VERSION}" > version.txt

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
mkdir -p pluto && (
    cd pluto
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
mkdir -p dof && (
    cd dof
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
mkdir -p qaqc && (
    cd qaqc
    for table in qaqc_aggregate qaqc_expected qaqc_mismatch qaqc_null qaqc_outlier qaqc_bbl_diffs
    do
        csv_export $table
    done
)

wait 
cd ..

dcpy lifecycle builds artifacts builds upload -p db-pluto -a public-read
