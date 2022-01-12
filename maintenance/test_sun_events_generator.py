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
        suneventExpect = ephem.Observer()
        suneventExpect.lat = latitude
        suneventExpect.lon = longitude
        sunevent = SunEventsGenerator(latitude, longitude)

        self.assertEqual(sunevent.obs.lat, suneventExpect.lat)

    def test_sampleSunEventsLongitude(self):

        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        end = datetime(2022, 1, 12, 23, 59, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        suneventExpect = ephem.Observer()
        suneventExpect.lat = latitude
        suneventExpect.lon = longitude
        sunevent = SunEventsGenerator(latitude, longitude)

        self.assertEqual(sunevent.obs.lon, suneventExpect.lon)

    def test_nextSunrise(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        sunevent = SunEventsGenerator(latitude, longitude)
        nextRiseDt = sunevent.next_sunrise(start)
        nextRiseDtExpected = datetime(
            2022, 1, 12, 7, 15, 23, 823969, tzinfo=timezone.utc)
        self.assertEqual(nextRiseDt, nextRiseDtExpected)

    def test_nextSunset(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        sunevent = SunEventsGenerator(latitude, longitude)
        nextSunsetDt = sunevent.next_sunset(start)
        nextSunsetDtExpected = datetime(
            2022, 1, 12, 16, 38, 52, 449657, tzinfo=timezone.utc)
        self.assertEqual(nextSunsetDt, nextSunsetDtExpected)

    def test_generateOneSunEvents(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        end = datetime(2022, 1, 12, 23, 59, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        sunevent = SunEventsGenerator(latitude, longitude)
        sunevents = sunevent.generate_sunevents(start, end)
        expectedSunEvents = [
            (datetime(2022, 1, 12, 7, 15, 23, 823969, tzinfo=timezone.utc),
             datetime(2022, 1, 12, 16, 38, 52, 465178, tzinfo=timezone.utc))]

        self.assertListEqual(sunevents, expectedSunEvents)

    def test_generateMultipleSunEvents(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        end = datetime(2022, 1, 14, 23, 59, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        sunevent = SunEventsGenerator(latitude, longitude)
        sunevents = sunevent.generate_sunevents(start, end)
        expectedSunEvents = [
            (datetime(2022, 1, 12, 7, 15, 23, 823969, tzinfo=timezone.utc),
             datetime(2022, 1, 12, 16, 38, 52, 465178, tzinfo=timezone.utc)),
            (datetime(2022, 1, 13, 7, 15, 2, 635578, tzinfo=timezone.utc),
             datetime(2022, 1, 13, 16, 39, 59, 562688, tzinfo=timezone.utc)),
            (datetime(2022, 1, 14, 7, 14, 39, 150562, tzinfo=timezone.utc),
             datetime(2022, 1, 14, 16, 41, 7, 649381, tzinfo=timezone.utc))
             ]

        self.assertListEqual(sunevents, expectedSunEvents)
