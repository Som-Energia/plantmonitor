 SELECT date_trunc('year'::text, result.day_date) AS year_date,
    (sum(result.energy_inversor) * (100)::numeric) AS energy_inversor_wh,
    result.inverter_name
   FROM ( SELECT date_trunc('day'::text, sistema_inversor."time") AS day_date,
            max(sistema_inversor.daily_energy_l) AS energy_inversor,
            sistema_inversor.inverter_name
           FROM public.sistema_inversor
          WHERE (sistema_inversor.location = 'Alcolea'::text)
          GROUP BY (date_trunc('day'::text, sistema_inversor."time")), sistema_inversor.inverter_name
          ORDER BY (date_trunc('day'::text, sistema_inversor."time"))) result
  GROUP BY (date_trunc('year'::text, result.day_date)), result.inverter_name
  ORDER BY (date_trunc('year'::text, result.day_date));


