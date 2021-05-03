SELECT sum(export_energy_wh)/1000000.0 AS energy_mwh
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id=reg.meter
LEFT JOIN plant ON plant.id=meter.plant
WHERE plant.name='{{ planta }}'
LIMIT 1000;