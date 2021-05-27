SELECT
    date_trunc('month', view_ht_daily."time") AS "time",
    view_ht_daily.plant,
    sum(hd),
    sum(ht),
    sum(hd)::float/sum(ht) AS "availability"
 FROM view_ht_daily
 JOIN view_hd_daily
    ON view_hd_daily.time = view_ht_daily.time
    and view_hd_daily.plant = view_ht_daily.plant
    group by date_trunc('month', view_ht_daily."time"), view_ht_daily.plant;