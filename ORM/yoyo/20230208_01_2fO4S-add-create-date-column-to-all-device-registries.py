"""
Add create_date column to all device registries
"""

from yoyo import step

__depends__ = {'20230131_01_dgwww-add-create-date-column-to-meter-registry'}

steps = [
    step(
        """
            alter table sensorirradiationregistry add create_date timestamp with time zone;
            alter table sensortemperatureambientregistry add create_date timestamp with time zone;
            alter table sensortemperaturemoduleregistry add create_date timestamp with time zone;
            alter table stringregistry add create_date timestamp with time zone;
            alter table inverterregistry add create_date timestamp with time zone;
            UPDATE sensorirradiationregistry SET create_date = time;
            UPDATE sensortemperatureambientregistry SET create_date = time;
            UPDATE sensortemperaturemoduleregistry SET create_date = time;
            UPDATE stringregistry SET create_date = time;
            UPDATE inverterregistry SET create_date = time;
        """,
        """
            alter table sensorirradiationregistry drop column create_date;
            alter table sensortemperatureambientregistry drop column create_date;
            alter table sensortemperaturemoduleregistry drop column create_date;
            alter table stringregistry drop column create_date;
            alter table inverterregistry drop column create_date;
        """
    )
]
