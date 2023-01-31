"""
Add create_date column to meter_registry
"""

from yoyo import step

__depends__ = {'20220504_01_2jUiZ-change-optional-to-set', '20220504_01_93Dar-allow-null-values-on-monthly-energy-estimates'}

steps = [
    step(
        """
        alter table meterregistry add create_date timestamp with time zone;
        UPDATE meterregistry SET create_date = time;
        """,
        """
        alter table meterregistry drop column create_date;
        """
    )
]
