# SQL Reference

## PostgreSQL

### Helpful Queries
#### Roles/users/permissions

```sql
-- Create a database cluster role
SELECT * FROM pg_roles;
CREATE ROLE readonly;
GRANT CONNECT ON DATABASE db-pluto TO readonly;
GRANT CONNECT ON DATABASE db-colp TO readonly;
GRANT CONNECT ON DATABASE db-devdb TO readonly;

-- Grant privileges to a role
-- Queries about schemas will be applied to the database of the current conenction
GRANT USAGE ON SCHEMA nightly_qa TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA nightly_qa TO readonly;

-- Create a user assigned to a role
SELECT * FROM pg_users;
CREATE USER first_last WITH PASSWORD 'A_GOOD_PASSWORD';
GRANT readonly TO first_last;
```

#### Storage/size
```sql
-- See the size of all schemas in a database
SELECT schema_name,
       pg_size_pretty(sum(table_size)::bigint),
       (sum(table_size) / pg_database_size(current_database())) * 100 as percent_of_db
FROM (
  SELECT pg_catalog.pg_namespace.nspname as schema_name,
         pg_relation_size(pg_catalog.pg_class.oid) as table_size
  FROM   pg_catalog.pg_class
     JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
) t
GROUP BY schema_name
ORDER BY schema_name;

-- Check size of indices
select 
    i.schemaname,
    i.relname AS table_name,
    indexrelname AS index_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS table_total_size,
    pg_size_pretty(pg_indexes_size(relid)) AS table_size_all_indices,
    pg_size_pretty(pg_relation_size(relid)) AS table_size,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
FROM pg_stat_all_indexes i 
INNER JOIN pg_class c ON i.relid = c.oid 
WHERE i.relname = '{ your table }'
```

#### Data Comparisons
```sql
-- Check if two tables are identical or different
WITH table1_check AS (
    SELECT COUNT(*) as row_count, 
           MD5(CAST((array_agg(t.*::text ORDER BY t.*::text)) AS text)) as checksum
    FROM shema_1.table_name t
),
table2_check AS (
    SELECT COUNT(*) as row_count,
           MD5(CAST((array_agg(t.*::text ORDER BY t.*::text)) AS text)) as checksum 
    FROM shema_2.table_name t
)
SELECT 
    CASE 
        WHEN t1.row_count = t2.row_count AND t1.checksum = t2.checksum
        THEN 'Tables are identical'
        ELSE 'Tables are different'
    END as comparison_result
FROM table1_check t1
CROSS JOIN table2_check t2;
```

```sql
-- Compare two tables in different schemas and return records
(SELECT *, 'Only in original' as difference
 FROM original_shema.table_name
 EXCEPT
 SELECT *, 'Only in original'
 FROM modified_shema.table_name)

UNION all

(SELECT *, 'Only in modified'
 FROM modified_shema.table_name
 EXCEPT
 SELECT *, 'Only in modified'
 FROM original_shema.table_name);
```

## MSSQL

### Restoring a backup using docker

To restore an mssql backup, the following steps can be taken.

First, pull/run the image
```bash
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=[my_secure_pw]" -p 1433:1433 -v [path_to_folder_with_backup_file]:/tmp/db_backup -d mcr.microsoft.com/mssql/server
```
- `mcr.microsoft.com/mssql/server` is the image we want
- set your pw - mssql will error if it is less than 8 characters and doesn't contain at least 3 different types of characters (lowercase, uppercase, number, special)
- you'll want to mount a volume if you are restoring a backup as give the database access to the file

Then, connect to your database with your method of choice. The default user is "sa".

Then, you can run the following commands. I've added some comments with what this looked like restoring AIMS data
```tsql
-- First, inspect the backup file
RESTORE FILELISTONLY
FROM DISK = '/tmp/db_backup/AIMS_backup_2025_01_04_070429_9170129.bak' ;
-- AIMS	     F:\SQL Data\AIMS.mdf
-- AIMS_log  F:\SQL Logs\AIMS_log.ldf

-- Without telling it where to put data, it errors
-- because it uses the filepaths for storing data found in the backup file
RESTORE DATABASE AIMS
FROM DISK = '/tmp/db_backup/AIMS_backup_2025_01_04_070429_9170129.bak';
-- " Directory lookup for the file "F:\SQL Data\AIMS.mdf" failed with the operating system error 2(The system cannot find the file specified.)."

-- Run command telling it where to store data.
-- this docker image stores data in /var/opt/mssql/data/ and logs in /var/opt/mssql/log/ by default
RESTORE DATABASE AIMS
FROM DISK = '/tmp/db_backup/AIMS_backup_2025_01_04_070429_9170129.bak'
WITH MOVE 'AIMS' TO '/var/opt/mssql/data/AIMS.mdf', 
     MOVE 'AIMS_log' TO '/var/opt/mssql/log/AIMS_log.ldf'
```
