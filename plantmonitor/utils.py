from datetime import datetime, timezone

def rfc3336todt(datetimestr):
    return datetime.strptime(datetimestr, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)