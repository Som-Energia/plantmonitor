#!/bin/bash
sudo su postgres -c "psql -d plantmonitor -f $1"
