"""
Timescale
"""

from yoyo import step

__depends__ = {'20210413_00-full-db'}

steps = [
    step("""
    SELECT
        create_hypertable(name::text, 'time', if_not_exists=>TRUE)
    FROM (VALUES
        ('meterregistry'),
        ('inverterregistry'),
        ('sensorirradiationregistry'),
        ('sensortemperatureambientregistry'),
        ('sensortemperaturemoduleregistry'),
        ('integratedirradiationregistry'),
        ('inclinometerregistry'),
        ('anemometerregistry'),
        ('omieregistry'),
        ('marketrepresentativeregistry'),
        ('simelregistry'),
        ('nagiosregistry')
    ) AS tables(name)
    WHERE EXISTS(
        SELECT proname
        FROM pg_catalog.pg_proc
        WHERE proname = 'create_hypertable'
    );
    """)
]
