# PlantMonitor

Through Plant Monitor, a tool to obtain data from the PV plant inverters,
Plant Monitor will connect directly to your inverter via Modbus TCP.

![Plant Schematics](/docs/plantmonitor.png?raw=true "Plant Schematics")

The tool is designed to allow any inverter enabled for Modbus TCP to be consulted by
using its own Modbus register map file.

To store the data consulted in the inverters, you can use TimescaleDB.
Finally, a control panel tool is needed to visualize the stored data, such as Grafana

## Pre-requisites

The Inverter must be accessible on the network using TCP.

This script should work on most Inverters that talk Modbus TCP. You can
customise your own modbus register file.

Install the required Python libraries for pymodbus:

```
pip install -r requirements.txt
```

Requires python >= 3.5

## Testing

Testing requires:

- The running user requires createdb grants in postgres (`ALTER USER myuser CREATEDB;`)
- `createdb orm_test`
- Copy `.env.example` as `.env.testing` and change the content
- Copy `conf/config.example.py` as `conf/config.py` and change the content
- Copy `conf/config_meteologica.example.yaml` as `conf/config_meteologica.yaml` and change the content

A series of mock, modbus sensors are available under `testingtools`.
Both client and server modbus can be simulated.

## Modifying the schema

Any change on the schema will modify `ORM/schema.sql` whenever
you run the test or explicitly run `scripts/update_schema.py`.

Those changes should be transfered to a new yoyo migration.

yoyo new -m "Some description"

Review and provide a forward and backward sql command for the step.

## Installation

1. Download or clone this repository to your local workstation. Install the
required libraries (see Pre-requisites section above).

2. Update the config.py with your values, such as the Inverter's IP address,
port, inverter model (which corresponds to the modbus register file) and the
register addresses Plant Monitor should scan from. Update the .env files with
values of your setup (use .env.example as reference to create .env.devel and .env.production).

3. Setup the database

```
psql -U postgres -h localhost
CREATE database plants;
\c plants
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```

To apply migrations the database

```bash
yoyo apply -d postgres://postgres:mypassword@localhost/plants ORM/yoyo
```


4. Run the project.

Scripts
=======

To write all solar events for a plant run:

```
$ python scripts/sun_events.py -p Alibaba
```

you can know the options running
```
$ python scripts/sun_events.py --help
```

Api
===

To run the api locally

```
~$ uvicorn api_server.plantmonitor_api:api --reload
```

Supervisor configuration
------------------------

```
[fcgi-program:plantmonitor_api]
socket=tcp://localhost:8000
command=/home/plantmonitor/Envs/plantmonitor/bin/uvicorn --fd 0 api_server.plantmonitor_api:api
numprocs=4
process_name=uvicorn-%(process_num)d
environment=PATH="/home/plantmonitor/Envs/plantmonitor/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
stdout_logfile=/var/log/supervisor/api_plantmonitor.log
stderr_logfile=/var/log/supervisor/api_plantmonitor_stderr.log
stdout_logfile_maxbytes=0
stderr_logfile_maxbytes=0
autostart=true
autorestart=true
directory=/home/plantmonitor/somenergia/plantmonitor
user=plantmonitor
```

Upgrading
=========

- Obtain the latest changes with `git fetch`
- Upgrade the repository: `git rebase master`
- Activate env: `workon plantmonitor`
- Install the dependencies: `pip install -r requirements.txt ; python ./setup.py develop`
- Apply yoyo migration: `yoyo apply`
- Publish the new views: `cd queries/;  ./publish.sh name_of_view.sql`
- Start monitoring the logs in background: `multitail /var/log/supervisor/plantmonitor_stderr.log /var/log/supervisor/api_plantmonitor*`
- Restart services: `sudo supervisorctl restart all`
- Check for startup errors in the service logs
- Wait for the first insert in the api from the plants to return 200 OK
- Check insert on database or database connection (checks models) with `python check_db_connection.py`
- Do the same process on every raspberrypi if the models changed

Troubleshooting
===============

* psycopg2 vs psycopg2-binary

At the moment we are using psycopg2-binary to simplify the installation process.
However at some point we will switch and you will need to install libpq-dev via apt. Changing between
psycopg2 and psycopg2-binary requires force reinstalling psycopg2. Also be certain
that the postgresql installation works with the timescale extension and its postgres
configuration.

* psycopg Python.h error

```
sudo apt install python3.7-dev
```

3.7 or the version you're using

* ModuleNotFoundError: No module named 'apt_pkg'

```
sudo apt install python3-apt --reinstall
```
