#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta, timezone

import ephem

class SunEventsGenerator:

    def __init__(self, latitude, longitude):
        self.obs = ephem.Observer()
        self.obs.lat = str(latitude)
        self.obs.lon = str(longitude)
        self.obs.elev = 0
        self.obs.pressure = 0
        # The United States Naval Observatory, rather than computing refraction dynamically,
        # uses a constant estimate of 34’ of refraction at the horizon. So in the above example,
        # rather than attempting to jury-rig values for temp and pressure that yield the magic 34’,
        # we turn off PyEphem refraction entirely and define the horizon itself as being at 34’
        # altitude instead.
        self.obs.horizon = '4:00' # sunrise based on U.S. Naval Astronomical Almanac values

    def next_sunrise(self, start):
        next_rise = self.obs.next_rising(ephem.Sun(), start=start, use_center=False)
        next_rise_dt = next_rise.datetime().replace(tzinfo=timezone.utc)
        return next_rise_dt

    def next_sunset(self, start):
        next_set = self.obs.next_setting(ephem.Sun(), start=start, use_center=False)
        next_set_dt = next_set.datetime().replace(tzinfo=timezone.utc)
        return next_set_dt

    def generate_sunevents(self, start=None, end=None):
        start = start or datetime.now(datetime.timezone.utc).replace(hour=00, minute=00, second=00)
        end = end or datetime.now(datetime.timezone.utc).replace(hour=23, minute=59, second=00)

        time_cursor = start
        sunevents = []

        while time_cursor < end:
            next_rise = self.next_sunrise(time_cursor)
            if next_rise > end:
                break
            time_cursor = next_rise
            next_set = self.next_sunset(time_cursor)
            time_cursor = next_set
            sunevents.append((next_rise,next_set))

        return sunevents



