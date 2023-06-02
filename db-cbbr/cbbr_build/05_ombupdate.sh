#!/bin/bash

# make sure we are at the top of the git directory
REPOLOC="$(git rev-parse --show-toplevel)"
cd $REPOLOC

cd '/prod/data-loading-scripts'

# load lookup table
echo 'Loading core datasets'
node loader.js install cbbr_omblookuptable
# load omb table
node loader.js install cbbr_ombresponse

cd '/prod/db-cbbr'

# load config
DBNAME=$(cat $REPOLOC/cbbr.config.json | jq -r '.DBNAME')
DBUSER=$(cat $REPOLOC/cbbr.config.json | jq -r '.DBUSER')

# update cbbr_submissions table with new OMB data
echo 'Updating data...'
psql -U $DBUSER -d $DBNAME -f $REPOLOC/cbbr_build/sql/update_omb.sql

# re normalize data
echo 'Normalizing data...'
psql -U $DBUSER -d $DBNAME -f $REPOLOC/cbbr_build/sql/normalize_agency.sql
#psql -U $DBUSER -d $DBNAME -f $REPOLOC/cbbr_build/sql/normalize_agencyacro.sql
psql -U $DBUSER -d $DBNAME -f $REPOLOC/cbbr_build/sql/normalize_commdist.sql
psql -U $DBUSER -d $DBNAME -f $REPOLOC/cbbr_build/sql/normalize_denominator.sql
psql -U $DBUSER -d $DBNAME -f $REPOLOC/cbbr_build/sql/normalize_sitetype.sql
