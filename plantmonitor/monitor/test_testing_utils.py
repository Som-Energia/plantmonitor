import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from unittest import TestCase
from pathlib import Path
from .testing_utils import (
    readTimedDataTsv,
    parseDate,
)
from yamlns import namespace as ns
from yamlns.dateutils import Date
import datetime
from decimal import Decimal

class TestingUtils_Test(TestCase):
    from b2btest.b2btest import assertB2BEqual

    @classmethod
    def setUpClass(cls):
        ''

    def test_readTimedDatTsv(self):
        testfile = Path("test.tsv")
        testfile.write_text(
            "Juny 2020														\n"
            "Dia	Hora	Temperatura modul	Irradiación (W/m2)	Isc en la radiación (A)	Isc a la temperatura (A)	Voc en la temperatura (V)	Imp temp (A)	Vmp temp (V)	P unitaria temp (W)	Potencia parque calculada con temperatura (kW)	Potencia instantanea inversors (kW)	Diferencia inversors vs Pcalculada	Potencia instanea a comptador	PR  %\n"
            "1/6/2020	0:05	15	34	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%\n"
            "1/6/2020	0:10	16	23	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%\n"
        )

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
        testfile.write_text(
            "Juny 2020														\n"
            "Dia	Hora	Temperatura modul	Irradiación (W/m2)	Isc en la radiación (A)	Isc a la temperatura (A)	Voc en la temperatura (V)	Imp temp (A)	Vmp temp (V)	P unitaria temp (W)	Potencia parque calculada con temperatura (kW)	Potencia instantanea inversors (kW)	Diferencia inversors vs Pcalculada	Potencia instanea a comptador	PR  %\n"
            "1/6/2020	0:05	15,1	34,5	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%\n"
            "1/6/2020	0:10	16,1	23,4	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%\n"
        )

        data = readTimedDataTsv('test.tsv', [
            "Temperatura modul",
            "Irradiación (W/m2)",
            ])
        self.assertEqual(data, [
            ["2020-06-01T00:05:00+02:00", 15.1, 34.5],
            ["2020-06-01T00:10:00+02:00", 16.1, 23.4],
        ])
        testfile.unlink()


    def test_readTimedDatTsv_spanishThousandPoint(self):
        testfile = Path("test.tsv")
        testfile.write_text(
            "Juny 2020														\n"
            "Dia	Hora	Temperatura modul	Irradiación (W/m2)	Isc en la radiación (A)	Isc a la temperatura (A)	Voc en la temperatura (V)	Imp temp (A)	Vmp temp (V)	P unitaria temp (W)	Potencia parque calculada con temperatura (kW)	Potencia instantanea inversors (kW)	Diferencia inversors vs Pcalculada	Potencia instanea a comptador	PR  %\n"
            "1/6/2020	0:05	1.005,1	34,5	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%\n"
            "1/6/2020	0:10	1.006,1	23,4	0	0	39,13056	0	31,26304	0	0	0	0,00%	0	0,00%\n"
        )

        data = readTimedDataTsv('test.tsv', [
            "Temperatura modul",
            "Irradiación (W/m2)",
            ])
        self.assertEqual(data, [
            ["2020-06-01T00:05:00+02:00", 1005.1, 34.5],
            ["2020-06-01T00:10:00+02:00", 1006.1, 23.4],
        ])
        testfile.unlink()


    def test_parseDate_spanishDate_localTime(self):
        self.assertEqual(
            parseDate("1/6/2020", "0:05"),
            "2020-06-01T00:05:00+02:00") # TODO: Check timezone!

    def test_parseDate_isoDate(self):
        self.assertEqual(
            parseDate("2020-06-01", "0:05"),
            "2020-06-01T00:05:00+02:00") # TODO: Check timezone!

    def test_parseDate_timeZoned(self):
        self.assertEqual(
            parseDate("2020-06-01", "0:05+3"),
            "2020-06-01T00:05:00+03:00") # TODO: Check timezone!

    def test_parseDate_detailedTimeZone(self):
        self.assertEqual(
            parseDate("2020-06-01", "0:05+2:30"),
            "2020-06-01T00:05:00+02:30") # TODO: Check timezone!
