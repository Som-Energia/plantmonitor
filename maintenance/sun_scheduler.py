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
        print(next_rise)
        return next_rise_dt

    def generate_sunevents(self, start=None, end=None):
        start = datetime.now(datetime.timezone.utc)
        step = timedelta(hours='1h')

        time_cursor = start
        sunevents = []

        while time_cursor < end:
            next_rise = self.next_sunrise(start)
            sunevents.append(next_rise)
            time_cursor = next_rise + step

        return sunevents



