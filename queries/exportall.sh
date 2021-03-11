#!/bin/bash
# Export all views in the database as sql files

# Should export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

./list.sh | while read v; do ./export.sh $v; done

