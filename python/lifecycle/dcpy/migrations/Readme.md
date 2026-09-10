# Migrations Directory

This directory contains SQL migration files used to create database schemas and tables. Each SQL file follows a strict naming convention to ensure orderly migration execution.

## File Naming Convention

Each migration file is named in the following format: `YYYYMMDDHH_{schema}__{table/view}.sql`.

## Usage

Run the SQL files in chronological order by timestamp to create or update database as required.
