#!/bin/bash
viewname=${1%.sql}
filename=$viewname.sql
sudo su postgres -c "psql -d plantmonitor -f $filename"
