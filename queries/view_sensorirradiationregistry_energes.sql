SELECT reg.sensor,
   reg."time",
   reg.irradiation_w_m2,
   reg.temperature_dc
  FROM sensorirradiationregistry as reg
  left join sensor on sensor.id = reg.sensor
  left join plant on plant.id = sensor.plant
  WHERE plant.id in (select id from view_plants_energes);
