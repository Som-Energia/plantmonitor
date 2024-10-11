#!/bin/bash
set -e

# create roles used in plantmonitor db

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	-- Create schema and roles
	-- nothing to do here
EOSQL
