--select plant.name as "plant_name::multiFilter", device_id, device_name, alarm.name as alarm_name, start_time, update_time,
--wait for redashv10 where multiFilters are "selectAll" by default
select * from (
    select plant.id as plant_id, plant.name as plant_name, device_id, device_name, alarm.name as alarm_name, start_time, update_time,
        case 
        when status = False THEN '<div class="bg-success p-10 text-center">OK</div>'
        WHEN status = True THEN '<div class="bg-danger p-10 text-center">ALARMA!</div>'
        WHEN status is NULL THEN '<div class="bg-warning p-10 text-center">Sense dades</div>'
        END as status,
        case 
        when status = False THEN 'OK'
        WHEN status = True THEN 'ALARMA!'
        WHEN status is NULL THEN 'Sense dades'
        END as _status
    from alarm_status
    left join alarm on alarm_status.alarm = alarm.id
    left join string on string.id = alarm_status.device_id and device_table = 'string'
    left join inverter on inverter.id = alarm_status.device_id and device_table = 'inverter' or inverter.id = string.inverter
    --left join meter on meter.id = alarm_status.device_id and device_table = 'meter'
    left join plant on inverter.plant = plant.id
) sub
where ('All' IN ({{ status }}) OR _status IN ({{ status }}))
AND (0 IN ({{ plant }}) OR plant_id IN ({{ plant }}));