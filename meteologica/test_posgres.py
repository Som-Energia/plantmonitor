#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from yamlns import namespace as ns

import psycopg2
from psycopg2 import Error

configdb = ns.load('config.yaml')

# Connexió postgresql
with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO conditions(time, device_id, temperature, humidity) VALUES (NOW(), 'weather-pro-000000', 84.1, 84.1);")
