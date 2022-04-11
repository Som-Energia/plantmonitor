select plant.name as plant_name, device_id, device_name, alarm.name as alarm_name, start_time, end_time
from alarm_historic
left join alarm on alarm_historic.alarm = alarm.id
left join inverter on inverter.id = alarm_historic.device_id and device_table = 'inverter'
--left join meter on meter.id = alarm_historic.device_id and device_table = 'meter'
left join plant on inverter.plant = plant.id
order by end_time desc;