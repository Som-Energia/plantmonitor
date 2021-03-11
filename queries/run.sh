#!/bin/bash
# Runs the sql file

# Should export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

viewname=${1%.sql}
filename=$viewname.sql
psql -d $PGDATABASE -f "$filename"
