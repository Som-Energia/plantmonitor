#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import ephem

class SunGenerator:

    def __init__(self):
        self.obs = ephem.Observer()
        self.obs.lat = '41.967599'
        self.obs.long = '2.837782'
        self.obs.elev = 0
        self.obs.pressure = 0
        # The United States Naval Observatory, rather than computing refraction dynamically,
        # uses a constant estimate of 34’ of refraction at the horizon. So in the above example,
        # rather than attempting to jury-rig values for temp and pressure that yield the magic 34’,
        # we turn off PyEphem refraction entirely and define the horizon itself as being at 34’
        # altitude instead.
        self.obs.horizon = '-0:34' # sunrise

    def next_sunrise(self, start):
        next_rise = self.obs.next_rising(ephem.Sun(), start=start, use_center=False)
        next_rise_dt = ephem.localtime(next_rise)
        print(next_rise_dt)
        return next_rise_dt

    def next_sunset(self, start):
        next_set = self.obs.next_rising(ephem.Sun(), start=start, use_center=False)
        next_set_dt = ephem.localtime(next_set)
        print(next_set_dt)
        return next_set_dt

    def generate_sunevents(self, start=None, end=None):
        start = datetime.now(datetime.timezone.utc)

        time_cursor = start
        sunevents = []

        while time_cursor < end:
            next_rise = self.next_sunrise(time_cursor)
            time_cursor = next_rise
            next_set = self.next_sunset(time_cursor)
            time_cursor = next_set
            sunevents.append((next_rise,next_set))

        return sunevents



