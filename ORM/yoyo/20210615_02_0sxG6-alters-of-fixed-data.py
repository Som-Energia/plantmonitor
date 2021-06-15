"""
alters of fixed data
"""

from yoyo import step

__depends__ = {'20210615_01_SbFVs-plantparameters'}

# set value
# set not null
# or set default value...


steps = [
    step('''
        ALTER TABLE plantmoduleparameters
        ADD COLUMN "brand" TEXT NOT NULL,
        ADD COLUMN "model" TEXT NOT NULL,
        ADD COLUMN "nominal_power_wp" INTEGER DEFAULT 250000 NOT NULL,
        ADD COLUMN "efficency_cpercent" INTEGER DEFAULT 1550 NOT NULL,
        ADD COLUMN "max_power_temperature_coefficient_mpercent_c" INTEGER DEFAULT -442 NOT NULL;

        ALTER TABLE inverter
        ADD COLUMN "brand" TEXT NOT NULL,
        ADD COLUMN "model" TEXT NOT NULL,
        ADD COLUMN "nominal_power_w" INTEGER;
        ''','''
        ALTER TABLE plantmoduleparameters
        DROP COLUMN "brand",
        DROP COLUMN "model",
        DROP COLUMN "nominal_power_wp",
        DROP COLUMN "efficency_cpercent",
        DROP COLUMN "max_power_temperature_coefficient_mpercent_c";

        ALTER TABLE inverter
        DROP COLUMN "brand",
        DROP COLUMN "model",
        DROP COLUMN "nominal_power_w";
    ''')
]
