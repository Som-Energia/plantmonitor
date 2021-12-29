CREATE OR REPLACE FUNCTION update_5min_average()
  RETURNS trigger AS $update_5min_average$
BEGIN
   INSERT INTO sensorirradiationregistry_5min_avg
         SELECT
               sir.sensor,
               time_bucket('5 minutes', sir.time::timestamptz) as time5min,
               avg(sir.irradiation_w_m2)::int as irradiation_w_m2,
               avg(sir.temperature_dc)::int as temperature_dc
            FROM sensorirradiationregistry as sir
            WHERE
               time_bucket('5 minutes', sir.time::timestamptz) = time_bucket('5 minutes', NEW.time::timestamptz) and
               sir.sensor = NEW.sensor
            GROUP by sir.sensor, time_bucket('5 minutes', sir.time::timestamptz)
      ON CONFLICT (sensor, time) DO UPDATE
         SET irradiation_w_m2 = excluded.irradiation_w_m2,
            temperature_dc = excluded.temperature_dc;
	RETURN NEW;
END;
$update_5min_average$ LANGUAGE plpgsql;

CREATE TRIGGER update_5min_average
  AFTER INSERT ON sensorirradiationregistry
  FOR EACH ROW
  EXECUTE PROCEDURE update_5min_average();

