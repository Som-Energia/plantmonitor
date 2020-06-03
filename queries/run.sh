#!/bin/bash
# Runs the sql file

# Should export PGHOST PGPORT PGUSER PGDATABASE PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

viewname=${1%.sql}
filename=$viewname.sql
sudo su postgres -c "psql -d plantmonitor -f $filename"
