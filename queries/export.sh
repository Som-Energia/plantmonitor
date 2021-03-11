#!/bin/bash
# Export a view as sql file

# Should export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

echo -e "\033[34mExporting $1\033[0m"

pg_dump --schema-only -d $PGDATABASE --if-exists --clean -t $1 |
	sed '/CREATE VIEW/,/ALTER TABLE/{//!b};d' |                                                                                            
	cat > "$1.sql"
