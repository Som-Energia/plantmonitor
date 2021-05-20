 SELECT sum(integrated_sensors.integral_irradiation_wh_m2) AS monthly_acumulated_irradiation,
    date_trunc('month'::text, integrated_sensors."time") AS monthly
   FROM public.integrated_sensors
  GROUP BY (date_trunc('month'::text, integrated_sensors."time"));


