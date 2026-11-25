-- Our own app data (the version catalog, ...) in its own database and role, apart from Dagster's.
-- Runs at first init (docker-entrypoint-initdb.d) and from the one-shot db-init service on every
-- `up`, so it has to be idempotent. psql only: \gexec runs each generated statement.
SELECT 'CREATE ROLE de_apps LOGIN PASSWORD ''de_apps'''
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'de_apps')\gexec

SELECT 'CREATE DATABASE de_apps OWNER de_apps'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'de_apps')\gexec

-- As on Azure, the owner can't write to public in a new database, so nothing may rely on it.
\connect de_apps
ALTER SCHEMA public OWNER TO CURRENT_USER;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
