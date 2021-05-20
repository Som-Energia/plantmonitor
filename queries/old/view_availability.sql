 SELECT (((view_hd_monthly.hd)::numeric * 1.0) / (view_ht_monthly.ht)::numeric) AS availability_percent,
    view_ht_monthly."time"
   FROM public.view_ht_monthly,
    public.view_hd_monthly
  WHERE ((view_ht_monthly."time" = view_hd_monthly."time") AND (view_hd_monthly.contador = '501600324'::text));


