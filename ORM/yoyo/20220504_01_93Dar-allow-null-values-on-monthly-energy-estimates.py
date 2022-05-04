"""
allow NULL values on monthly energy estimates
"""

from yoyo import step

__depends__ = {'20220420_01_up8HQ-add-target-historic-energy-plant'}

steps = [
    step('''
    ALTER TABLE "plantestimatedmonthlyenergy"
        ALTER COLUMN "monthly_target_energy_kwh" TYPE BIGINT,
        ALTER COLUMN "monthly_target_energy_kwh" DROP NOT NULL,
        ALTER COLUMN "monthly_historic_energy_kwh" TYPE BIGINT,
        ALTER COLUMN "monthly_historic_energy_kwh" DROP NOT NULL;
    ''', '''
    ALTER TABLE "plantestimatedmonthlyenergy"
        ALTER COLUMN "monthly_target_energy_kwh" TYPE BIGINT,
        ALTER COLUMN "monthly_target_energy_kwh" SET NOT NULL,
        ALTER COLUMN "monthly_historic_energy_kwh" TYPE BIGINT,
        ALTER COLUMN "monthly_historic_energy_kwh" SET NOT NULL;
    ''')
]
