--- This is a dummy view to tcheck the timetz problem
SELECT foo.month
   FROM ( SELECT date_trunc('month'::text, deprecated_view_pr_hourly_poltergeist.timezone) AS month
           FROM public.deprecated_view_pr_hourly_poltergeist
          GROUP BY (date_trunc('month'::text, deprecated_view_pr_hourly_poltergeist.timezone))) foo;


