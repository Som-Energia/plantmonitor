 SELECT
        CASE
            WHEN (deprecated_view_pr_hourly_raw.pr_hourly IS NULL) THEN (0)::double precision
            WHEN ((1.05)::double precision < deprecated_view_pr_hourly_raw.pr_hourly) THEN (1.0)::double precision
            ELSE deprecated_view_pr_hourly_raw.pr_hourly
        END AS pr_hourly,
    timezone('Europe/Madrid'::text, sistema_contador."time") AS timezone,
    sistema_contador.name AS contador
   FROM public.sistema_contador,
    public.deprecated_view_pr_hourly_raw
  WHERE ((timezone('UTC'::text, sistema_contador."time") = deprecated_view_pr_hourly_raw.timezone) AND (sistema_contador.name = '501600324'::text));


