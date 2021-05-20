 SELECT count(integrated_sensors.integral_irradiation_wh_m2) AS ht,
    date_trunc('month'::text, integrated_sensors."time") AS "time"
   FROM public.integrated_sensors
  WHERE ((integrated_sensors.integral_irradiation_wh_m2)::double precision > (5)::double precision)
  GROUP BY (date_trunc('month'::text, integrated_sensors."time"));


