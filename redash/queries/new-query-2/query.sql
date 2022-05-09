select sum(export_energy_wh) from meterregistry join meter on meterregistry.meter = meter.id where meter = 2 order by time asc limit 1000;

