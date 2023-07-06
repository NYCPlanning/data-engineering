#!/bin/bash
DIR=$(pwd)
NAME="pluto_pts"

function import_pts {
   # create temporary location
   VERSION=$(date "+%Y%m%d")

   mkdir -p /tmp/pts &&
   (
      cd /tmp/pts

      ssh_cmd get Prod_FromDOF/PTS_Propmast.gz .
      gunzip PTS_Propmast.gz

      # remove last column of pts --> empty column
      cut -d$'\t' -f1-139 PTS_Propmast > pluto_pts.csv

      # remove spaces between delimiters
      sed -i 's/ *\t/\t/g' pluto_pts.csv
      sed -i 's/\"//g' pluto_pts.csv

      # Check number of rows
      wc -l pluto_pts.csv

      # Check number of columns
      awk -F'\t' '{print NF; exit}' pluto_pts.csv

      # Load to psql
      cat pluto_pts.csv | psql $RECIPE_ENGINE -v NAME=$NAME -f $DIR/sql/_load_pts.sql
      rm pluto_pts.csv

      # Create Outputs in preparation for data library
      psql $RECIPE_ENGINE -1 -c "\COPY ( 
      SELECT * FROM $NAME ) TO stdout DELIMITER ',' CSV HEADER;" > pluto_pts.csv
      psql $RECIPE_ENGINE -1 -c "\COPY ( 
      SELECT DISTINCT ON (boro, block, lot) boro, block, lot 
      FROM $NAME ) TO stdout DELIMITER ',' CSV HEADER;" > geocode_input_pluto_pts.csv
      mc cp pluto_pts.csv spaces/edm-recipes/tmp/pluto_pts.csv
   )
}
register 'import' 'pts' 'import pts' import_pts

function geocode_pts {
   cp /tmp/pts/geocode_input_pluto_pts.csv $(pwd)/python/geocode_input_pluto_pts.csv
   rm -rf /tmp/pts/pluto_input_geocodes.csv
   docker run --rm\
      --user $(id -u):$(id -g)\
      -v $(pwd):/project\
      -w /project/python\
      -e RECIPE_ENGINE=$RECIPE_ENGINE\
      nycplanning/docker-geosupport:latest python3 geocode.py
   rm $(pwd)/python/geocode_input_pluto_pts.csv
   mc cp $(pwd)/python/pluto_input_geocodes.csv spaces/edm-recipes/tmp/pluto_input_geocodes.csv
}
register 'geocode' 'pts' 'geocode pts' geocode_pts 


function clean_pts {
   rm -rf /tmp/pts
   mc rm spaces/edm-recipes/tmp/pluto_input_geocodes.csv
   mc rm spaces/edm-recipes/tmp/pluto_pts.csv
}
register 'clean' 'pts' 'clean pts' clean_pts 
