 SELECT view_alcolea_energy_inverter_monthly.month_date,
    (view_alcolea_energy_inverter_monthly.energy_inversor_wh / NULLIF(result.inverter_total, (0)::numeric)) AS inverter_energy_percent,
    view_alcolea_energy_inverter_monthly.inverter_name
   FROM public.view_alcolea_energy_inverter_monthly,
    ( SELECT view_alcolea_energy_inverter_monthly_1.month_date,
            sum(view_alcolea_energy_inverter_monthly_1.energy_inversor_wh) AS inverter_total
           FROM public.view_alcolea_energy_inverter_monthly view_alcolea_energy_inverter_monthly_1
          GROUP BY view_alcolea_energy_inverter_monthly_1.month_date) result
  WHERE (result.month_date = view_alcolea_energy_inverter_monthly.month_date);


