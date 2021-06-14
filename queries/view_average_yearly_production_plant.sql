SELECT
    yearly.plant AS plant,
    avg(export_energy_mwh) as historic_avg,
    time_year as time
FROM (
    SELECT
        plant.name as plant,
        (avg(export_energy_wh) / 1000000.0) * 24 * 365 AS export_energy_mwh,
        date_trunc('year', time) AS time_year,
        extract(year FROM time) AS year
    FROM
        meterregistry
    LEFT JOIN meter ON meterregistry.meter = meter.id
    LEFT JOIN plant ON meter.plant = plant.id
WHERE
    time IS NOT NULL
    AND extract(year FROM time) < extract(year FROM now())
GROUP BY
    plant.name,
    time_year,
    year) AS yearly
GROUP BY
    yearly.plant, time_year;
