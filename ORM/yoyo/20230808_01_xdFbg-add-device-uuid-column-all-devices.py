"""
Add device_uuid column to all device types (plant, meter, inverter, string, sensor).
Create signal_device_relation table.
"""

from yoyo import step

__depends__ = {'20210920_03_BXEr2-timescale-stringregistry'}

steps = [
    step(
        """
        ALTER TABLE plant ADD device_uuid VARCHAR(36);
        ALTER TABLE meter ADD device_uuid VARCHAR(36);
        ALTER TABLE inverter ADD device_uuid VARCHAR(36);
        ALTER TABLE string ADD device_uuid VARCHAR(36);
        ALTER TABLE sensor ADD device_uuid VARCHAR(36);

        CREATE TABLE "signal_device_relation" (
            "id" SERIAL PRIMARY KEY,
            "signal_uuid" VARCHAR(36) NOT NULL,
            "signal_name" TEXT,
            "device_uuid" VARCHAR(36) NOT NULL,
            "description" TEXT
        );
        """,
        """
        ALTER TABLE plant DROP COLUMN device_uuid;
        ALTER TABLE meter DROP COLUMN device_uuid;
        ALTER TABLE inverter DROP COLUMN device_uuid;
        ALTER TABLE string DROP COLUMN device_uuid;
        ALTER TABLE sensor DROP COLUMN device_uuid;
        DROP TABLE signal_device_relation;
        """
    )
]
