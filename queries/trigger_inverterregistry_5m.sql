CREATE OR REPLACE FUNCTION update_5min_average()
  RETURNS trigger AS $update_5min_average$
BEGIN
   INSERT INTO inverterregistry_5min_avg
         SELECT
               sir.sensor,
               time_bucket('5 minutes', sir.time::timestamptz) as time,
               avg(sir.power_w)::int as power_w,
               max(sir.energy_wh)::int as energy_wh
            FROM inverterregistry as sir
            WHERE
               time_bucket('5 minutes', sir.time::timestamptz) = time_bucket('5 minutes', NEW.time::timestamptz) and
               sir.sensor = NEW.sensor
            GROUP by sir.sensor, time_bucket('5 minutes', sir.time::timestamptz)
      ON CONFLICT (sensor, time) DO UPDATE
         SET power_w = excluded.power_w,
            energy_wh = excluded.energy_wh;
	RETURN NEW;
END;
$update_5min_average$ LANGUAGE plpgsql;

CREATE TRIGGER update_5min_average
  AFTER INSERT ON inverterregistry
  FOR EACH ROW
  EXECUTE PROCEDURE update_5min_average();
