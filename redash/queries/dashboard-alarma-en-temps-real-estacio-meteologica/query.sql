SELECT *
FROM query_158
WHERE temps BETWEEN datetime('now', 'start of day') AND datetime('now', '+1 day')
  and current_ok is null or current_ok= ''
and plant_id in ({{ plant_id }});
