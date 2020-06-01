 SELECT date_trunc('year'::text, view_pr_monthly.month_date) AS year,
    avg(view_pr_monthly.pr_monthly) AS pr_yearly_average_percent,
    view_pr_monthly.contador
   FROM public.view_pr_monthly
  WHERE (view_pr_monthly.contador = '501600324'::text)
  GROUP BY (date_trunc('year'::text, view_pr_monthly.month_date)), view_pr_monthly.contador
  ORDER BY (date_trunc('year'::text, view_pr_monthly.month_date));


