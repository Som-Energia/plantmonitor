#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta, timezone
import ephem
import unittest
from .sun_events_generator import SunEventsGenerator


# TODO: tests generate_sunevents

class Sun_events_generator_Test(unittest.TestCase):

    def setUp(self):
        pass


    def test_sampleSunEventsLatitude(self):

        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        end = datetime(2022, 1, 12, 23, 59, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        suneventsExpect = ephem.Observer()
        suneventsExpect.lat = latitude
        suneventsExpect.lon = longitude
        sunevents = SunEventsGenerator(latitude, longitude)

        self.assertEqual(sunevents.obs.lat, suneventsExpect.lat)

    def test_sampleSunEventsLongitude(self):

        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        end = datetime(2022, 1, 12, 23, 59, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        suneventsExpect = ephem.Observer()
        suneventsExpect.lat = latitude
        suneventsExpect.lon = longitude
        sunevents = SunEventsGenerator(latitude, longitude)

        self.assertEqual(sunevents.obs.lon, suneventsExpect.lon)

    def test_nextSunrise(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        sunevents = SunEventsGenerator(latitude, longitude)
        nextRiseDt = sunevents.next_sunrise(start)
        nextRiseDtExpected = datetime(
            2022, 1, 12, 7, 15, 23, 823969, tzinfo=timezone.utc)
        self.assertEqual(nextRiseDt, nextRiseDtExpected)

    def test_nextSunset(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        sunevents = SunEventsGenerator(latitude, longitude)
        nextSunsetDt = sunevents.next_sunset(start)
        nextSunsetDtExpected = datetime(
            2022, 1, 12, 16, 38, 52, 449657, tzinfo=timezone.utc)
        self.assertEqual(nextSunsetDt, nextSunsetDtExpected)