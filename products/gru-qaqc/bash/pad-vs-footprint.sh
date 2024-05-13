#!/bin/bash
set -e
source bash/config.sh
parse_flags $@

function pull {
  local boro=$1
  if ! [ -f rejects_padbin_footprintbin.csv ]; then
    echo "bin,boro,block,lot,lhnd,hhnd,stname" > rejects_padbin_footprintbin.csv
  fi
  psql $BUILD_ENGINE -c "\COPY (
      WITH output AS (
      SELECT
              bin, boro, block, 
              lot, lhnd, hhnd, stname,
              ROW_NUMBER () OVER ( 
          PARTITION BY bin, boro, block, lot
        ) as row
            FROM (
              SELECT 
                bin, boro, block, 
                lot,lhnd, hhnd, stname,
                substring(bin from 2 for 3)::integer as bin_num
              FROM dcp_pad
              WHERE left(bin, 1) = '$boro'
                AND bin NOT IN (
                SELECT DISTINCT(b.bin::bigint::text)
                FROM doitt_buildingfootprints b
                WHERE left(bin::bigint::text, 1) = '$boro')
              AND substring(bin from 2 for 6)::integer != 0
            ) a WHERE bin_num != ALL(ARRAY[799,798,797,796]))
            
      SELECT bin, boro, block, lot, lhnd, hhnd, stname
      FROM output
      WHERE row = '1'
    ) TO STDOUT DELIMITER ',' CSV;" >> rejects_padbin_footprintbin.csv
}

# Data loading
if [[ $import_data -eq 1 ]]; then 
    import doitt_buildingfootprints 
    import dcp_pad $(get_geosupport_version)
fi

# Execute
if [[ $python_script -eq 1 ]]; then 
  echo "no python script to execute here ..."
fi

# Export file
if [[ $export_data -eq 1 ]]; then 
    mkdir -p output/pad-vs-footprint && (
      cd output/pad-vs-footprint
      pull "1"
      wait
      pull "2" &
      pull "3" &
      pull "4" &
      pull "5" &
      wait
      CSV_export versions
    )
fi

# Upload File
if [[ $upload_data -eq 1 ]]; then 
    Upload pad-vs-footprint latest
    Upload pad-vs-footprint $DATE
fi