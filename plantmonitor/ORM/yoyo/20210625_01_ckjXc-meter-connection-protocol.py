"""
Meter connection protocol
"""

from yoyo import step

__depends__ = {'20210517_01_uwtNO-expected-power-correction-factor-cpercent'}

steps = [
    step('''
        ALTER TABLE meter
        ADD COLUMN "connection_protocol" VARCHAR DEFAULT 'ip' NOT NULL;
        ''',
        '''
        ALTER TABLE meter
        DROP COLUMN "connection_protocol";
        '''
    )
]
