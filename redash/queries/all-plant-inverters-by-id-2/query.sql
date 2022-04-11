select inverter.id, inverter.name
from inverter
where inverter.plant = {{ plant }}
