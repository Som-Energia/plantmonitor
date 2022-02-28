 SELECT plant.name AS plant_name,
    inverter.name AS inverter_name,
    reg.inverter,
    reg."time",
    reg.power_w,
    reg.energy_wh,
    reg.intensity_cc_ma,
    reg.intensity_ca_ma,
    reg.voltage_cc_mv,
    reg.voltage_ca_mv,
    reg.uptime_h,
    reg.temperature_dc
   FROM inverterregistry reg
     LEFT JOIN inverter ON inverter.id = reg.inverter
     LEFT JOIN plant ON plant.id = inverter.plant
  WHERE plant.name in ('Alcolea', 'Florida', 'Matallana');
