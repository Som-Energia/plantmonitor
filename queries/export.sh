#!/bin/bash

sudo su postgres -c "pg_dump --schema-only -d plantmonitor --if-exists --clean -t $1" |
	sed '/CREATE VIEW/,/ALTER TABLE/{//!b};d' |                                                                                            
	cat > "$1.sql"
