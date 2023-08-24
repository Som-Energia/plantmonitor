"""
Add device_uuid column to all device types (plant, meter, inverter, string, sensor).
Create signal_device_relation table.
"""

from yoyo import step

__depends__ = {'20230808_01_xdFbg-add-device-uuid-column-all-devices'}

steps = [
    step(
        """
        DROP TABLE signal_device_relation;
        CREATE TABLE "signal_device_relation" (
            "id" SERIAL PRIMARY KEY,
            "plant" TEXT,
            "signal_description" TEXT,
            "device_description" TEXT,
            "signal_uuid" uuid NOT NULL,
            "device_uuid" uuid NOT NULL
        );

        """,
        """
        DROP TABLE signal_device_relation;
        CREATE TABLE "signal_device_relation" (
            "id" SERIAL PRIMARY KEY,
            "signal_uuid" uuid NOT NULL,
            "signal_name" TEXT,
            "device_uuid" uuid NOT NULL,
            "description" TEXT
        );
        """
    )
]
