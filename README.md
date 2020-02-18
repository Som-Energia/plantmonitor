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

Install the required Python libraries for pymodbus and influxdb:

```
pip install -r requirements.txt
```

## Testing

A series of mock, modbus sensors are available under `testingtools`.
Both client and server modbus can be simulated.

## Installation

1. Download or clone this repository to your local workstation. Install the 
required libraries (see Pre-requisites section above).

2. Update the config.py with your values, such as the Inverter's IP address, 
port, inverter model (which corresponds to the modbus register file) and the
register addresses Plant Monitor should scan from.

3. Run the project.
