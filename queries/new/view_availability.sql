 SELECT
    view_ht_daily."time" AS "time",
    plant,
    hd/ht AS "availability"
 FROM view_ht_daily
 JOIN view_hd_daily
    ON view_hd_daily.time = view_ht_daily.time
    and view_hd_daily.plant = view_ht_daily.plant;