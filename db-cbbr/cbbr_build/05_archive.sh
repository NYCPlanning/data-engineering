#!/bin/bash
source config.sh

## to be finished
pg_dump -t cbbr_submissions --no-owner $BUILD_ENGINE | psql $EDM_DATA
DATE=$(date "+%Y/%m/%d");
psql $EDM_DATA -c "CREATE SCHEMA IF NOT EXISTS cbbr_submissions;";
psql $EDM_DATA -c "SELECT * INTO cbbr_submissions.cbbr_submissions FROM cbbr_submissions;";
psql $EDM_DATA -c "DROP TABLE cbbr_submissions;";
psql $EDM_DATA -c "DROP VIEW IF EXISTS cbbr_submissions.latest;";
psql $EDM_DATA -c "DROP TABLE IF EXISTS cbbr_submissions.\"$DATE\";";
psql $EDM_DATA -c "ALTER TABLE cbbr_submissions.cbbr_submissions RENAME TO \"$DATE\";";
psql $EDM_DATA -c "CREATE VIEW cbbr_submissions.latest AS (SELECT * FROM cbbr_submissions.\"$DATE\");";

pg_dump -t geo_rejects --no-owner $BUILD_ENGINE | psql $EDM_DATA
psql $EDM_DATA -c "DROP TABLE IF EXISTS cbbr_submissions.geo_rejects;";
psql $EDM_DATA -c "SELECT * INTO cbbr_submissions.geo_rejects FROM geo_rejects;";
psql $EDM_DATA -c "DROP TABLE IF EXISTS geo_rejects;";