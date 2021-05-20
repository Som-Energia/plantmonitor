 SELECT forecastdata.idforecasthead,
    forecastdata."time",
    forecastdata.percentil50,
    forecasthead.facilityid
   FROM public.forecastdata,
    public.forecasthead
  WHERE (forecastdata.idforecasthead = forecasthead.id)
  ORDER BY forecastdata."time", forecasthead.facilityid;


