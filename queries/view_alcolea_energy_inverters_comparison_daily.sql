 SELECT view_alcolea_energy_inverter_daily.day_date,
    ((view_alcolea_energy_inverter_daily.energy_inversor_wh)::numeric / NULLIF(result.inverter_total, (0)::numeric)) AS inverter_energy_percent,
    view_alcolea_energy_inverter_daily.inverter_name
   FROM public.view_alcolea_energy_inverter_daily,
    ( SELECT view_alcolea_energy_inverter_daily_1.day_date,
            sum(view_alcolea_energy_inverter_daily_1.energy_inversor_wh) AS inverter_total
           FROM public.view_alcolea_energy_inverter_daily view_alcolea_energy_inverter_daily_1
          GROUP BY view_alcolea_energy_inverter_daily_1.day_date) result
  WHERE (result.day_date = view_alcolea_energy_inverter_daily.day_date);


