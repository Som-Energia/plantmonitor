#!/bin/bash
viewname=${1%.sql}
echo La vista seria \"$viewname\"
VIEWSQL=$(cat $1)

sudo su postgres -c "psql -d plantmonitor" <<EOF
DROP VIEW IF EXISTS public.$viewname;
CREATE VIEW public.$viewname AS
$VIEWSQL

ALTER TABLE public.$viewname OWNER TO postgres;
GRANT ALL ON TABLE public.$viewname TO meteologica;
GRANT ALL ON TABLE public.$viewname TO grafana;
EOF

