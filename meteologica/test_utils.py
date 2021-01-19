import unittest

from meteologica.utils import shiftOneHour, todt


class UtilsTest(unittest.TestCase):

    def test_shiftOneHour(self):
        data = {
            "fooFacility": [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
            "booFacility": [
                (todt("2040-01-02 00:00:00"), 210),
                (todt("2040-01-02 01:00:00"), 340),
            ],
        }

        expectedData = {
            "fooFacility": [
                (todt("2040-01-01 23:00:00"), 10),
                (todt("2040-01-02 00:00:00"), 20),
            ],
            "booFacility": [
                (todt("2040-01-01 23:00:00"), 210),
                (todt("2040-01-02 00:00:00"), 340),
            ],
        }

        shiftedData = shiftOneHour(data)

        self.assertDictEqual(expectedData, shiftedData)
