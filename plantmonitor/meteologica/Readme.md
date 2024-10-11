Meteologica API client
=====================

Setup
-----

* [Timescaledb on Ubuntu 18.04](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-timescaledb-on-ubuntu-18-04)

```
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt update
sudo apt install timescaledb-postgresql-10
sudo apt install timescaledb-tools
sudo timescaledb-tune
```

```
sudo systemctl restart postgresql.service
```

```
sudo -u postgres psql
```

```
CREATE DATABASE meteologica;
\c meteologica
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```

Create table with whichever fields you want

```
CREATE TABLE conditions (
  time        TIMESTAMP WITH TIME ZONE NOT NULL,
  device_id   TEXT,
  temperature  NUMERIC,
  humidity     NUMERIC
);
```

```
SELECT create_hypertable('conditions', 'time');
```

Modify privileges of postgres DATABASE edit `/etc/postgresql/11/main/pg_hba.conf`

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
#host    all             all             127.0.0.1/32            md5
host    meteologica    postgres        127.0.0.1/32            trust
```

or whatever config rocks your boat.

* python Setup

```
mkvirtualenv -p $(which python3) --system-site-packages meteo
pip install zeep yamlns psycopg2
```

* Insert values to Timescaledb using `psycopg2`

```
from yamlns import namespace as ns

import psycopg2
from psycopg2 import Error

configdb = ns.load('config.yaml')

with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO conditions(time, device_id, temperature, humidity) VALUES (NOW(), 'weather-pro-000000', 84.1, 84.1);")
```
