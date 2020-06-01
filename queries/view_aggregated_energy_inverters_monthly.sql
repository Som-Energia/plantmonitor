 SELECT date_trunc('month'::text, view_alcolea_energy_inverter_monthly.month_date) AS month_date,
    sum(view_alcolea_energy_inverter_monthly.energy_inversor_wh) AS energy_inverters_wh
   FROM public.view_alcolea_energy_inverter_monthly
  GROUP BY view_alcolea_energy_inverter_monthly.month_date;


