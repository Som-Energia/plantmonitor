import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from unittest import TestCase
from ORM.models import (
    database,
    Plant,
    SensorIrradiation,
    SensorIrradiationRegistry,
)
from pony import orm
from pathlib import Path
from .expectedpower import (
    readTimedDataTsv,
    spanishDateToISO,
)
from yamlns import namespace as ns
import datetime
from ORM.orm_util import setupDatabase
setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)


class ExpectedPower_Test(TestCase):

    @classmethod
    def setUpClass(cls):
        ''

    def setUp(self):
        self.maxDiff=None
        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        database.drop_all_tables(with_all_data=True)

        orm.set_sql_debug(True)
        database.create_tables()

        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def samplePlantNS(self): # Copied from test_models.py:samplePlantNS
        alcoleaPlantNS = ns.loads("""\
            name: myplant
            codename: PLANTCODE
            description: Plant Description
            meters:
            - meter:
                name: '1234578'
            irradiationSensors:
            - irradiationSensor:
                name: irradiationSensor1
        """)
        return alcoleaPlantNS

    def test_readTimedDatTsv(self):
        testfile = Path("test.tsv")
        testfile.write_text("""\
Juny 2020														
Dia	Hora	Temperatura modul	Irradiación (W/m2)	Isc en la radiación (A)	Isc a la temperatura (A)	Voc en la temperatura (V)	Imp temp (A)	Vmp temp (V)	P unitaria temp (W)	Potencia parque calculada con temperatura (kW)	Potencia instantanea inversors (kW)	Diferencia inversors vs Pcalculada	Potencia instanea a comptador	PR  %
1/6/2020	0:05	15	34	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%
1/6/2020	0:10	16	23	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%
""")

        data = readTimedDataTsv('test.tsv', [
            "Temperatura modul",
            "Irradiación (W/m2)",
            ])
        self.assertEqual(data, [
            ["2020-06-01T00:05:00+02:00", 15, 34],
            ["2020-06-01T00:10:00+02:00", 16, 23],
        ])
        testfile.unlink()

    def test_readTimedDatTsv_spanishFloats(self):
        testfile = Path("test.tsv")
        testfile.write_text("""\
Juny 2020														
Dia	Hora	Temperatura modul	Irradiación (W/m2)	Isc en la radiación (A)	Isc a la temperatura (A)	Voc en la temperatura (V)	Imp temp (A)	Vmp temp (V)	P unitaria temp (W)	Potencia parque calculada con temperatura (kW)	Potencia instantanea inversors (kW)	Diferencia inversors vs Pcalculada	Potencia instanea a comptador	PR  %
1/6/2020	0:05	15,1	34,5	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%
1/6/2020	0:10	16,1	23,4	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%
""")

        data = readTimedDataTsv('test.tsv', [
            "Temperatura modul",
            "Irradiación (W/m2)",
            ])
        self.assertEqual(data, [
            ["2020-06-01T00:05:00+02:00", 15.1, 34.5],
            ["2020-06-01T00:10:00+02:00", 16.1, 23.4],
        ])
        testfile.unlink()


    def test_spanishDateToISO(self):
        self.assertEqual(
            spanishDateToISO("1/6/2020", "0:05"),
            "2020-06-01T00:05:00+02:00") # TODO: Check timezone!

    def setupPlant(self):
        plantDefinition = self.samplePlantNS()
        plant = Plant(
            name=plantDefinition.name,
            codename=plantDefinition.codename,
        )
        plant.importPlant(plantDefinition)
        plant.flush()
        self.sensor = SensorIrradiation.select().first().id

    def test_insertData(self):
        self.setupPlant()
        for time, irradiation_w_m2, temperature_c in readTimedDataTsv('expectedPower-2020-06-Alcolea.csv', [
            'Temperatura modul',
            'Irradiación (W/m2)',
        ]):
            print(time)
            SensorIrradiationRegistry(
                sensor=self.sensor,
                time=datetime.datetime.fromisoformat(time),
                irradiation_w_m2=irradiation_w_m2,
                temperature_dc=round(temperature_c*10),
            ).flush()




