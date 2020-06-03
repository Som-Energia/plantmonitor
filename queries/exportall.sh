#!/bin/bash
# Export all views in the database as sql files

# Should define environs PGHOST PGPORT PGUSER PGDATABASE PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

./list.sh | while read v; do ./export.sh $v; done

