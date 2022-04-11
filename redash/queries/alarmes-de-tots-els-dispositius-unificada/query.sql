select plant.name as plant_name, device_id, device_name, alarm.name as alarm_name, start_time, update_time,
    case 
    when status = False THEN '<div class="bg-success p-10 text-center">OK</div>'
    WHEN status = True THEN '<div class="bg-danger p-10 text-center">ALARMA!</div>'
    WHEN status is NULL THEN '<div class="bg-warning p-10 text-center">Sense dades</div>'
    END as status
from alarm_status
left join alarm on alarm_status.alarm = alarm.id
left join inverter on inverter.id = alarm_status.device_id and device_table = 'inverter'
--left join meter on meter.id = alarm_status.device_id and device_table = 'meter'
left join plant on inverter.plant = plant.id;