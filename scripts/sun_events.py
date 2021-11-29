
import os
ENVIRONMENT_VARIABLE = 'PLANTMONITOR_MODULE_SETTINGS'

os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')

import datetime
import click
from pony import orm

from maintenance.sun_events_generator import SunEventsGenerator
from ORM.pony_manager import PonyManager


class SunEvents():

    def sun_events_update(self, start, end):


        from conf import envinfo
        database_info = envinfo.DB_CONF

        solar_db = PonyManager(database_info)

        plants = orm.select(p for p in solar_db.db.Plant)[:]

        for plant in plants:
            lat = plant.location.latitude
            lon = plant.location.longitude
            sun_events_gen = SunEventsGenerator(lat,lon)

            sun_events = sun_events_gen.generate_sunevents(start=start,end=end)

            print(sun_events)

            # TODO insert to db

@click.command()
@click.option('-s', '--start', default=None, help='start date')
@click.option('-e', '--end', default=None, help='end date')
@click.option('-p', '--plant', multiple=True, help='Plant to update sunevents (accepts multiple)')
def plant_sun_events_update(start, end, plant):

    plants = plant
    start = datetime.datetime.strptime(start, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
    end = datetime.datetime.strptime(end, '%Y-%m-%d').replace(hour=23,minute=59,second=59,tzinfo=datetime.timezone.utc)

    # se = SunEvents()

    # se.sun_events_update()

    return 0

if __name__ == '__main__':

    plant_sun_events_update()
