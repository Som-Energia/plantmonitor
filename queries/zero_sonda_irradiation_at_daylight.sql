
select sensorirradiationregistry_5min_avg.sensor,
    sensorirradiationregistry_5min_avg.time as time,
    plant.id as plant,
    sensorirradiationregistry_5min_avg.irradiation_w_m2,
    sensorirradiationregistry_5min_avg.temperature_dc,
    solarevent.sunrise as sunrise,
    solarevent.sunset as sunset
    from sensorirradiationregistry_5min_avg
    left join sensor on sensor.id = sensorirradiationregistry_5min_avg.sensor
    left join plant on plant.id = sensor.plant
    left join solarevent on sensorirradiationregistry_5min_avg.time::date = solarevent.sunrise::date and solarevent.plant = plant.id
    where sensorirradiationregistry_5min_avg.irradiation_w_m2 = 0
    and sensorirradiationregistry_5min_avg.time between solarevent.sunrise and solarevent.sunset
    and sensorirradiationregistry_5min_avg.time > {}
    order by time desc
