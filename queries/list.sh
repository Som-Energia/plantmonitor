#!/bin/bash
# List all views in the database

# Should export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

psql -d $PGDATABASE -t -c 'select table_name from INFORMATION_SCHEMA.views WHERE table_schema = ANY (current_schemas(false));'
