#!/bin/bash

sudo su postgres -c "psql -d plantmonitor -c 'select table_name from INFORMATION_SCHEMA.views WHERE table_schema = ANY (current_schemas(false));'"
