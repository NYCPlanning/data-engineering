#!/bin/bash

# Script to set up permissions for specified user(s)
#
# This script configures database permissions with the following access model:
# - Tables users create: Full ownership (CREATE, DROP, ALTER, INSERT, UPDATE, DELETE, SELECT)
# - Tables created by others: DML operations only (INSERT, UPDATE, DELETE, SELECT)
# - Public schema: USAGE only (can use PostGIS/extensions, cannot create tables)
# - Functions: EXECUTE (needed for PostGIS functions)
#
# Users will have these restrictions:
# - NOSUPERUSER: Cannot perform admin operations
# - NOCREATEDB: Cannot create new databases
# - NOCREATEROLE: Cannot create/modify roles
# - Cannot DROP or ALTER tables they don't own
# - Cannot CREATE objects in public schema
#
# Requires superuser credentials in environment variables:
# - PGHOST
# - PGPORT (optional, defaults to 5432)
# - PGUSER (superuser)
# - PGPASSWORD
#
# Usage: ./setup_db_permissions.sh username1 [username2 ...]
# Example: ./setup_db_permissions.sh de_product_builds de_builds_bot

set -e

# Check if at least one username was provided
if [ $# -eq 0 ]; then
  echo "Error: Please provide at least one username"
  echo "Usage: $0 username1 [username2 ...]"
  echo "Example: $0 de_product_builds de_builds_bot"
  exit 1
fi

# List of databases to configure
DATABASES=(
  "db-cbbr"
  "db-cdbg"
  "db-ceqr"
  "db-checkbook"
  "db-colp"
  "db-cpdb"
  "db-cscl"
  "db-devdb"
  "db-facilities"
  "db-factfinder"
  "db-green-fast-track"
  "db-pluto"
  "db-template"
  "db-ztl"
  "dbt-de"
  "edm-qaqc"
  "edm-zap"
  "kpdb"
  "recipe"
)

# Users to configure from command line arguments
USERS=("$@")

echo "Setting up permissions for users: ${USERS[@]}"
echo "Databases: ${DATABASES[@]}"
echo ""

# Build comma-separated list of users for SQL statements
USER_LIST=$(printf ", %s" "${USERS[@]}")
USER_LIST=${USER_LIST:2}  # Remove leading ", "

# First, set user attributes (run once on any database)
echo "Setting user attributes..."
for USER in "${USERS[@]}"; do
  psql -d defaultdb -c "ALTER USER $USER WITH NOSUPERUSER NOCREATEDB NOCREATEROLE;" 2>/dev/null || echo "Note: User $USER may not exist yet"
done
echo ""

# Loop through each database and set permissions
for DB in "${DATABASES[@]}"; do
  echo "Configuring database: $DB"

  # Grant CONNECT on database
  for USER in "${USERS[@]}"; do
    psql -d defaultdb -c "GRANT CONNECT ON DATABASE \"$DB\" TO $USER;"
  done

  # Connect to the database and grant schema and table permissions
  psql -d "$DB" <<EOF
-- Grant schema permissions (USAGE only - no CREATE in public schema)
-- This allows using PostGIS and other extensions in public without polluting it
GRANT USAGE ON SCHEMA public TO $USER_LIST;

-- Grant permissions on existing tables in public schema
GRANT SELECT ON ALL TABLES IN SCHEMA public TO $USER_LIST;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $USER_LIST;

-- Grant permissions on future tables in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO $USER_LIST;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO $USER_LIST;

-- Grant permissions on sequences (needed for serial/auto-increment columns)
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $USER_LIST;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO $USER_LIST;

-- Grant permissions on functions (needed for PostGIS and other extensions)
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO $USER_LIST;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO $USER_LIST;
EOF

  echo "✓ Completed: $DB"
  echo ""
done

echo "All permissions configured successfully!"
