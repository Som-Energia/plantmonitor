with estimates (plant_name, time, monthly_target_energy_kwh, monthly_historic_energy_kwh) as
( VALUES
	('Alcolea', '2021-01-01' at time zone 'Europe/Madrid', 221730, 220590),
	('Alcolea', '2021-02-01' at time zone 'Europe/Madrid', 228930, 238150),
	('Alcolea', '2021-03-01' at time zone 'Europe/Madrid', 320480, 266710),
	('Alcolea', '2021-04-01' at time zone 'Europe/Madrid', 300310, 282320),
	('Alcolea', '2021-05-01' at time zone 'Europe/Madrid', 342790, 328360),
	('Alcolea', '2021-06-01' at time zone 'Europe/Madrid', 333270, 325790),
	('Alcolea', '2021-07-01' at time zone 'Europe/Madrid', 361990, 340700),
	('Alcolea', '2021-08-01' at time zone 'Europe/Madrid', 344380, 303400),
	('Alcolea', '2021-09-01' at time zone 'Europe/Madrid', 318790, 300450),
	('Alcolea', '2021-10-01' at time zone 'Europe/Madrid', 279880, 263200),
	('Alcolea', '2021-11-01' at time zone 'Europe/Madrid', 200210, 196780),
	('Alcolea', '2021-12-01' at time zone 'Europe/Madrid', 199280, 202300),
	('Fontivsolar', '2021-01-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-02-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-03-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-04-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-05-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-06-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-07-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-08-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-09-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-10-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-11-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Fontivsolar', '2021-12-01' at time zone 'Europe/Madrid', NULL, NULL),
	('Florida', '2021-01-01' at time zone 'Europe/Madrid', 161880, NULL),
	('Florida', '2021-02-01' at time zone 'Europe/Madrid', 170660, NULL),
	('Florida', '2021-03-01' at time zone 'Europe/Madrid', 244070, NULL),
	('Florida', '2021-04-01' at time zone 'Europe/Madrid', 243980, NULL),
	('Florida', '2021-05-01' at time zone 'Europe/Madrid', 280100, NULL),
	('Florida', '2021-06-01' at time zone 'Europe/Madrid', 280840, NULL),
	('Florida', '2021-07-01' at time zone 'Europe/Madrid', 302570, NULL),
	('Florida', '2021-08-01' at time zone 'Europe/Madrid', 276530, NULL),
	('Florida', '2021-09-01' at time zone 'Europe/Madrid', 245790, NULL),
	('Florida', '2021-10-01' at time zone 'Europe/Madrid', 211970, NULL),
	('Florida', '2021-11-01' at time zone 'Europe/Madrid', 146210, NULL),
	('Florida', '2021-12-01' at time zone 'Europe/Madrid', 143180, NULL),
	('Matallana', '2021-01-01' at time zone 'Europe/Madrid', 229940, 239580),
	('Matallana', '2021-02-01' at time zone 'Europe/Madrid', 235600, 269670),
	('Matallana', '2021-03-01' at time zone 'Europe/Madrid', 340040, 353700),
	('Matallana', '2021-04-01' at time zone 'Europe/Madrid', 327030, 320050),
	('Matallana', '2021-05-01' at time zone 'Europe/Madrid', 376290, 390720),
	('Matallana', '2021-06-01' at time zone 'Europe/Madrid', 374360, 392790),
	('Matallana', '2021-07-01' at time zone 'Europe/Madrid', 392740, 369800),
	('Matallana', '2021-08-01' at time zone 'Europe/Madrid', 352710, 369690),
	('Matallana', '2021-09-01' at time zone 'Europe/Madrid', 310860, 330410),
	('Matallana', '2021-10-01' at time zone 'Europe/Madrid', 290640, 316620),
	('Matallana', '2021-11-01' at time zone 'Europe/Madrid', 244730, 224650),
	('Matallana', '2021-12-01' at time zone 'Europe/Madrid', 218520, 217380)
)
insert into plantestimatedmonthlyenergy (plantparameters, time, monthly_target_energy_kwh, monthly_historic_energy_kwh)
    select pp.id, time, monthly_target_energy_kwh, monthly_historic_energy_kwh from estimates
	inner join plant on plant.name = estimates.plant_name
	inner join plantparameters pp on pp.plant = plant.id
	where pp.id is not null;

--select plant.name, pe.* from plantestimatedmonthlyenergy pe left join plantparameters pp on pp.id = pe.plantparameters left join plant on plant.id = pp.plant order by plant, time desc;