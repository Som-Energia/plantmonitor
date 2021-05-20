 SELECT date_trunc('month'::text, forecastdata."time") AS month_date,
    (sum(forecastdata.percentil50) * 1000) AS forecasted_energy_wh,
    forecasthead.facilityid
   FROM public.forecastdata,
    public.forecasthead
  WHERE (forecastdata.idforecasthead = forecasthead.id)
  GROUP BY (date_trunc('month'::text, forecastdata."time")), forecasthead.facilityid
  ORDER BY (date_trunc('month'::text, forecastdata."time"));


