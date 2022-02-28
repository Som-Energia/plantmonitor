#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timezone
import ephem
import unittest
from .sun_events_generator import SunEventsGenerator


class Sun_events_generator_Test(unittest.TestCase):

    def test_sampleSunEventsLatitude(self):

        latitude = '41.9818'
        longitude = '2.8237'
        suneventExpect = ephem.Observer()
        suneventExpect.lat = latitude
        suneventExpect.lon = longitude
        sunevent = SunEventsGenerator(latitude, longitude)

        self.assertEqual(sunevent.obs.lat, suneventExpect.lat)

    def test_sampleSunEventsLongitude(self):
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
            2022, 1, 12, 7, 44, 9, 306857, tzinfo=timezone.utc)
        self.assertEqual(nextRiseDt, nextRiseDtExpected)

    def test_nextSunset(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        sunevent = SunEventsGenerator(latitude, longitude)
        nextSunsetDt = sunevent.next_sunset(start)
        nextSunsetDtExpected = datetime(
            2022, 1, 12, 16, 10, 6, 787575, tzinfo=timezone.utc)
        self.assertEqual(nextSunsetDt, nextSunsetDtExpected)

    def test_generateOneSunEvents(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        end = datetime(2022, 1, 12, 23, 59, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        sunevent = SunEventsGenerator(latitude, longitude)
        sunevents = sunevent.generate_sunevents(start, end)
        expectedSunEvents = [
            (datetime(2022, 1, 12, 7, 44, 9, 306857, tzinfo=timezone.utc),
             datetime(2022, 1, 12, 16, 10, 6, 806238,tzinfo=timezone.utc))]

        self.assertListEqual(sunevents, expectedSunEvents)

    def test_generateMultipleSunEvents(self):
        start = datetime(2022, 1, 12, 00, 00, 00, 0, tzinfo=timezone.utc)
        end = datetime(2022, 1, 14, 23, 59, 00, 0, tzinfo=timezone.utc)
        latitude = '41.9818'
        longitude = '2.8237'
        self.maxDiff = None
        sunevent = SunEventsGenerator(latitude, longitude)
        sunevents = sunevent.generate_sunevents(start, end)
        expectedSunEvents = [
            (datetime(2022, 1, 12, 7, 44, 9, 306857, tzinfo=timezone.utc),
             datetime(2022, 1, 12, 16, 10, 6, 806238, tzinfo=timezone.utc)),
            (datetime(2022, 1, 13, 7, 43, 43, 609709, tzinfo=timezone.utc),
             datetime(2022, 1, 13, 16, 11, 18, 397711, tzinfo=timezone.utc)),
            (datetime(2022, 1, 14, 7, 43, 15, 498317,  tzinfo=timezone.utc),
             datetime(2022, 1, 14, 16, 12, 31, 96118, tzinfo=timezone.utc))
             ]

        self.assertListEqual(sunevents, expectedSunEvents)
