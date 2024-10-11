
from datetime import datetime, timedelta, timezone


def todt(datetimestr):
    if not datetimestr:
        return None
    # TODO remove this try-catch when the meteologica_port has been running in production and
    # lastdateFile timestamps are aware
    try:
        timestamp = datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        timestamp = datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S%z')
    return timestamp

def todtaware(datetimestr):
    if not datetimestr:
        return None
    return datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)


def tztodt(datetimestr):
    if not datetimestr:
        return None
    return datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S %Z')


def shiftOneHour(readings):
    delta = timedelta(hours=1)

    return {f: [(t-delta, v) for t,v in values] for f,values in readings.items()}

def shiftOneHourWithoutFacility(readings):
    delta = timedelta(hours=1)

    return [(t-delta, v) for t,v in readings]

def round_hour(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))