#!/usr/bin/env python
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')

import datetime

from ORM.models import database

from ORM.models import (
    Plant,
    Meter,
    MeterRegistry,
)

from ORM.orm_util import setupDatabase
setupDatabase()

if __name__ == "__main__":

    with orm.db_session:
        alcolea = Plant(name='SomEnergia_Alcolea', description='A fotovoltaic plant')
        meter = Meter(name='Mary', plant=alcolea)
        meterRegistry = MeterRegistry(
            meter=meter,
            time=datetime.datetime.now(),
            export_energy=10,
            import_energy=77,
            r1=0,
            r2=0,
            r3=0,
            r4=0,
        )
        #TODO add sensors

        alcolea_read = Plant[1]


    print('database created on import')


