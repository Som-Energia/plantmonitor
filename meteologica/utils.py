
from datetime import datetime

def todt(datetimestr):
    if not datetimestr:
        return None
    return datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S')

