#!/bin/bash
# Publishes the sql file as view in the database

# Should export PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD PGOPTIONS
[ -e dbconfig.sh ] && source dbconfig.sh

viewname=${1%.sql}
filename=$viewname.sql
echo Publishing \"$viewname\"
VIEWSQL=$(cat $filename)

psql -d $PGDATABASE <<EOF
DROP VIEW IF EXISTS public.$viewname;
CREATE VIEW public.$viewname AS
$VIEWSQL

ALTER TABLE public.$viewname OWNER TO postgres;
GRANT ALL ON TABLE public.$viewname TO meteologica;
GRANT ALL ON TABLE public.$viewname TO grafana;
EOF

