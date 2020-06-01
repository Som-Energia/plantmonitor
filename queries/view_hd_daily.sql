 SELECT count(view_pr_hourly.pr_hourly) AS hd,
    date_trunc('day'::text, view_pr_hourly.timezone) AS "time",
    view_pr_hourly.contador
   FROM public.view_pr_hourly
  WHERE (view_pr_hourly.pr_hourly > (0.70)::double precision)
  GROUP BY (date_trunc('day'::text, view_pr_hourly.timezone)), view_pr_hourly.contador;


