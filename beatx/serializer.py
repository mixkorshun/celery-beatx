from datetime import datetime

from celery.beat import ScheduleEntry
from celery.schedules import crontab, schedule, solar

__all__ = ('serialize_entry', 'deserialize_entry')

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


def serialize_entry(entry):
    """
    Serialize ScheduleEntry to json-valid dictionary.

    Helps serialize entry to json, yml and any other formats.

    :param entry: ScheduleEntry
    :return: json-valid dictionary
    """
    return {
        'name': entry.name,
        'task': entry.task,
        'schedule': encode_schedule(entry.schedule),
        'args': entry.args,
        'kwargs': entry.kwargs,
        'last_run_at': encode_datetime(entry.last_run_at),
        'total_run_count': entry.total_run_count,
        'options': entry.options
    }


def deserialize_entry(entry):
    """
    Deserialize ScheduleEntry from dictionary.

    Helps deserialize entry from json, yml and any other formats.

    :param entry:
    :return:
    """
    return ScheduleEntry(
        name=entry['name'],
        task=entry['task'],
        schedule=decode_schedule(entry['schedule']),
        args=entry['args'],
        kwargs=entry['kwargs'],
        last_run_at=decode_datetime(entry['last_run_at']),
        total_run_count=entry['total_run_count'],
        options=entry['options'],
    )


def encode_datetime(dt):
    return dt.strftime(DATETIME_FORMAT) if dt else None


def decode_datetime(s):
    return datetime.strptime(s, DATETIME_FORMAT) if s else None


def encode_schedule(value):
    if value is None:
        return None
    elif isinstance(value, datetime):
        return {
            '__type__': 'datetime',
            '__value__': encode_datetime(value)
        }
    elif isinstance(value, crontab):
        return {
            '__type__': 'crontab',
            '__value__': '%(minute)s\t%(hour)s\t%(day_of_week)s\t'
                         '%(day_of_month)s\t%(month_of_year)s' % {
                             'minute': value._orig_minute,
                             'hour': value._orig_hour,
                             'day_of_week': value._orig_day_of_week,
                             'day_of_month': value._orig_day_of_month,
                             'month_of_year': value._orig_month_of_year,
                         }
        }
    elif isinstance(value, solar):
        return {
            '__type__': 'solar',
            '__value__': {
                'event': value.event,
                'lat': value.lat,
                'lon': value.lon
            }
        }
    elif isinstance(value, schedule):
        return {
            '__type__': 'schedule',
            '__value__': {
                'run_every': value.run_every.total_seconds(),
                'relative': bool(value.relative),
            }
        }
    else:
        raise NotImplementedError(
            'Cannot serialize schedule %(type)s type' % {
                'type': type(value).__name__
            }
        )


def decode_schedule(obj):
    if obj is None:
        return None

    _type = obj['__type__']
    value = obj['__value__']

    if _type == 'datetime':
        return decode_datetime(value)
    elif _type == 'crontab':
        return crontab(*value.split('\t'))
    elif _type == 'solar':
        return solar(**value)
    elif _type == 'schedule':
        return schedule(**value)
    else:
        raise NotImplementedError(
            'Cannot deserialize schedule %(type)s type' % {
                'type': _type
            }
        )
