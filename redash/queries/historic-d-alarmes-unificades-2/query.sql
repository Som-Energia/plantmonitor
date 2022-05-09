select * from
(
    select plant.id as plant_id, plant.name as plant_name, device_id, device_name as device_name, alarm.name as alarm_name, start_time at time zone 'Europe/Madrid' as start_time, end_time at time zone 'Europe/Madrid' as end_time
    from alarm_historic
    left join alarm on alarm_historic.alarm = alarm.id
    left join inverter on inverter.id = alarm_historic.device_id and device_table = 'inverter'
    --left join meter on meter.id = alarm_historic.device_id and device_table = 'meter'
    left join plant on inverter.plant = plant.id
    order by end_time desc, plant, device_name
) sub
WHERE (0 IN ({{ plant }}) OR plant_id IN ({{ plant }}))
  AND (end_time at time zone 'Europe/Madrid' between '{{ interval.start }}' and '{{ interval.end }}');