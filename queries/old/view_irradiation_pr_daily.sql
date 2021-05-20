 SELECT sum(integrated_sensors.integral_irradiation_wh_m2) AS daily_acumulated_irradiation,
    date_trunc('day'::text, integrated_sensors."time") AS daily
   FROM public.integrated_sensors
  GROUP BY (date_trunc('day'::text, integrated_sensors."time"));


