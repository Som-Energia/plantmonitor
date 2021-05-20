 SELECT 
    date_trunc('day'::text, inverterregistry."time") AS day_date,
    inverter.name as inverter_name,
    (max(inverterregistry.energy_wh) * 100) AS energy_inversor_wh
   FROM  public.inverterregistry
   LEFT JOIN public.inverter
   ON inverter.id = inverterregistry.inverter
   LEFT JOIN public.plant
   ON plant.id = inverter.plant
  WHERE (plant.name = 'Alcolea'::text)
  GROUP BY (date_trunc('day'::text, inverterregistry."time")), inverter.name
  ORDER BY (date_trunc('day'::text, inverterregistry."time")), inverter.name;


