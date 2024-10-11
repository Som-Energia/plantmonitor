"""
Add stringbox_name of strings
"""

from yoyo import step

__depends__ = {'20210920_03_BXEr2-timescale-stringregistry'}

steps = [
    step('''
        ALTER TABLE string
        ADD COLUMN "stringbox_name" TEXT;
        ''',
        '''
        ALTER TABLE string
        DROP COLUMN "stringbox_name";
    ''')
]
