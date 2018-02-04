import json
from datetime import datetime

import pytest
from celery.beat import ScheduleEntry
from celery.schedules import crontab, solar, schedule

# noinspection PyProtectedMember
from beatx.serializer import encode_schedule, decode_schedule, \
    serialize_entry, deserialize_entry


@pytest.mark.parametrize('dt', [
    crontab('10', '*/2'),
    solar('sunset', -37.81753, 144.96715),
    schedule(17, relative=True)
])
def test_serialize_entry_json_serializable(dt):
    entry = ScheduleEntry(
        name='entry-1',
        task='entry-1-task',
        schedule=dt,
        args=('arg1', 'arg2'),
        kwargs={'key1': 'val1', 'key2': 'val2'},
        last_run_at=datetime.now(),
        total_run_count=1,
        options={},
    )

    obj = serialize_entry(entry)

    try:
        json.dumps(obj)
    except Exception as e:
        pytest.fail(e)


@pytest.mark.parametrize('dt', [
    crontab('10', '*/2'),
    solar('sunset', -37.81753, 144.96715),
    schedule(17, relative=True)
])
def test_serialize_deserialize_entry(dt):
    entry = ScheduleEntry(
        name='entry-1',
        task='entry-1-task',
        schedule=dt,
        args=('arg1', 'arg2'),
        kwargs={'key1': 'val1', 'key2': 'val2'},
        last_run_at=datetime.now(),
        total_run_count=1,
        options={},
    )

    decoded_entry = deserialize_entry(serialize_entry(entry))

    assert decoded_entry.__reduce__() == entry.__reduce__()


@pytest.mark.parametrize('value_type,value', [
    ('datetime', datetime(2016, 1, 5, 23, 30, 15)),
    ('crontab', crontab('10', '*/2')),
    ('solar', solar('sunset', -37.81753, 144.96715)),
    ('schedule', schedule(17, relative=True)),
])
def test_encode_schedule(value_type, value):
    encoded = encode_schedule(value)
    assert encoded['__type__'] == value_type


@pytest.mark.parametrize('value_type,value', [
    ('datetime', datetime(2016, 1, 5, 23, 30, 15)),
    ('crontab', crontab('10', '*/2')),
    ('solar', solar('sunset', -37.81753, 144.96715)),
    ('schedule', schedule(17, relative=True)),
])
def test_can_decode_encoded_schedule(value_type, value):
    decoded_value = decode_schedule(encode_schedule(value))

    assert decoded_value == value
