"""
Meter connection protocol
"""

from yoyo import step

__depends__ = {'20210517_01_uwtNO-expected-power-correction-factor-cpercent'}

steps = [
    step('''
        ALTER TABLE meters 
        ADD COLUMN "connection_protocol" VARCHAR DEFAULT 'ip' NOT NULL;
        ''',
        '''
        ALTER TABLE meters
        DROP COLUMN "connection_protocol";
        '''
    )
]
