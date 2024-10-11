"""
expected_power_correction_factor_cpercent
"""

from yoyo import step

__depends__ = {'20210420_01_Pqalq-hourlysensorirradiationregistry'}

steps = [
    step('''   
        ALTER TABLE plantmoduleparameters 
        ADD COLUMN "expected_power_correction_factor_cpercent" INTEGER DEFAULT 10000 NOT NULL;
        ''',
        '''
        ALTER TABLE plantmoduleparameters
        DROP COLUMN "expected_power_correction_factor_cpercent";
        '''
    )
]
