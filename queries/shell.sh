#!/bin/bash
# Runs the sql file

# Should export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

psql -d $PGDATABASE
