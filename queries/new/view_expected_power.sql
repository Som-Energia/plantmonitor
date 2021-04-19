SELECT
    time,
    sensor.plant AS plant,
    sensor.id AS sensor,
    CASE WHEN irradiation_w_m2 <=0 THEN 0 ELSE (
	par.n_modules
	* par.max_power_current_ma/1000.0
	* par.max_power_voltage_mv/1000.0
	* (
	    irradiation_w_m2 / par.standard_conditions_irradiation_w_m2::float +
	    (temperature_dc - par.standard_conditions_temperature_dc)/10.0 * par.current_temperature_coefficient_mpercent_c/100000.0
	)
	* (
	    1 +
	    (temperature_dc - par.standard_conditions_temperature_dc)/10.0 * par.voltage_temperature_coefficient_mpercent_c/100000.0
	)
	* par.degradation_cpercent/10000.0
	/ 1000.0   -- W -> kW
    ) 
    END AS expectedpower
    FROM sensorirradiationregistry AS reg
    LEFT JOIN sensor AS sensor
    ON sensor.id = reg.sensor
    LEFT JOIN plantmoduleparameters AS par
    ON par.plant = sensor.plant
    ORDER BY time, plant, sensor

--- Expected Power


