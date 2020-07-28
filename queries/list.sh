#!/bin/bash
# List all views in the database

# Should define environs PGHOST PGPORT PGUSER PGDATABASE PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

psql -d plantmonitor -t -c 'select table_name from INFORMATION_SCHEMA.views WHERE table_schema = ANY (current_schemas(false));'
