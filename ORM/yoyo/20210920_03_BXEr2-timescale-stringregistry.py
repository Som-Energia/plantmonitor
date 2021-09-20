"""
Timescale StringRegistry
"""

from yoyo import step

__depends__ = {'20210920_02_kFUDk-add-inverter-strings-tables'}

steps = [
    step("""
    SELECT create_hypertable('stringregistry', 'time', if_not_exists=>TRUE);
    """)
]

