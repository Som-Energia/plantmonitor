
from datetime import datetime, timedelta, timezone


def todt(datetimestr):
    if not datetimestr:
        return None
    return datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S')

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
