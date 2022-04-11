select 0 as id, 'All' as name
UNION
select id, name from plant
order by id
