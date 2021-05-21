 SELECT
    view_ht_monthly."time" AS "time",
    view_ht_monthly.plant,
    hd/ht AS "availability"
 FROM view_ht_monthly
 JOIN view_hd_monthly
    ON view_hd_monthly.time = view_ht_monthly.time
    and view_hd_monthly.plant = view_ht_monthly.plant;