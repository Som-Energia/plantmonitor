from integrate_csv import integrate, fillHoles
from unittest import TestCase

import datetime
import scipy
from itertools import groupby

class Integrate_Test(TestCase):

  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_AlcoleaIrradiance(self):

    self.maxDiff = None
    filename = 'b2bdata/irradiance-2021-07-21-Alcolea.csv'

    result = integrate(filename)

    # TODO make this a back to back
    expected = [
      (datetime.datetime(2021, 7, 14, 7, 0, tzinfo=datetime.timezone.utc), 690.912884450634),
      (datetime.datetime(2021, 7, 14, 6, 0, tzinfo=datetime.timezone.utc), 487.51614860550035),
      (datetime.datetime(2021, 7, 14, 5, 0, tzinfo=datetime.timezone.utc), 263.8896494526125),
      (datetime.datetime(2021, 7, 14, 4, 0, tzinfo=datetime.timezone.utc), 75.03673014458036),
      (datetime.datetime(2021, 7, 14, 3, 0, tzinfo=datetime.timezone.utc), 14.042129167704843),
      (datetime.datetime(2021, 7, 14, 1, 0, tzinfo=datetime.timezone.utc), 0.04164847225183621),
      (datetime.datetime(2021, 7, 14, 0, 0, tzinfo=datetime.timezone.utc), 0.0),
      (datetime.datetime(2021, 7, 13, 23, 0, tzinfo=datetime.timezone.utc), 0.0),
      (datetime.datetime(2021, 7, 13, 22, 0, tzinfo=datetime.timezone.utc), 0.0),
      (datetime.datetime(2021, 7, 13, 21, 0, tzinfo=datetime.timezone.utc), 0.0),
      (datetime.datetime(2021, 7, 13, 20, 0, tzinfo=datetime.timezone.utc), 0.0),
      (datetime.datetime(2021, 7, 13, 19, 0, tzinfo=datetime.timezone.utc), 0.0)
    ]

    self.assertListEqual(result[0:12], expected)

  def test_fillHoles(self):
    timeseries = [
      (datetime.datetime(2021, 7, 14, 7, 0, tzinfo=datetime.timezone.utc), 120),
      (datetime.datetime(2021, 7, 14, 7, 20, tzinfo=datetime.timezone.utc), 130),
    ]

    result = fillHoles(timeseries)

    expected = [
      (datetime.datetime(2021, 7, 14, 7, 0, tzinfo=datetime.timezone.utc), 120),
      (datetime.datetime(2021, 7, 14, 7, 5, tzinfo=datetime.timezone.utc), 0),
      (datetime.datetime(2021, 7, 14, 7, 10, tzinfo=datetime.timezone.utc), 0),
      (datetime.datetime(2021, 7, 14, 7, 15, tzinfo=datetime.timezone.utc), 0),
      (datetime.datetime(2021, 7, 14, 7, 20, tzinfo=datetime.timezone.utc), 130),
    ]

    self.assertListEqual(result, expected)