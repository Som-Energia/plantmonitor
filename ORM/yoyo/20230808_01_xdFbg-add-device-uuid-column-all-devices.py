"""
Add device_uuid column to all device types (plant, meter, inverter, string, sensor)
"""

from yoyo import step

__depends__ = {'20210920_03_BXEr2-timescale-stringregistry'}

steps = [
    step(
        """
        alter table plant add device_uuid varchar(36);
        alter table meter add device_uuid varchar(36);
        alter table inverter add device_uuid varchar(36);
        alter table string add device_uuid varchar(36);
        alter table sensor add device_uuid varchar(36);
        """,
        """
        alter table plant drop column device_uuid;
        alter table meter drop column device_uuid;
        alter table inverter drop column device_uuid;
        alter table string drop column device_uuid;
        alter table sensor drop column device_uuid;
        """
    )
]
