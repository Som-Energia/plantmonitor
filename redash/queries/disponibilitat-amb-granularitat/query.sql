SELECT
    date_trunc('{{ granularity }}', htreg."time") AS "time",
    htreg.plant,
    sum(hd)::int as hd,
    sum(ht)::int as ht,
    sum(hd)/sum(ht) AS availability
 FROM view_ht_daily as htreg
 JOIN view_hd_daily hdreg
    ON hdreg.time = htreg.time
    and hdreg.plant = htreg.plant
where htreg.plant = '{{ plant }}' AND
    htreg."time" >= '{{ interval.start }}' AND
    htreg."time" <= '{{ interval.end }}'
    group by date_trunc('{{ granularity }}', htreg."time"), htreg.plant;
    