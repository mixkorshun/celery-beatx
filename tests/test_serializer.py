from datetime import datetime

import pytest
from celery.schedules import crontab, solar, schedule

from beatx.serializer import encode_datetime, decode_datetime, encode_schedule, decode_schedule


class TestEncodingDatetime:
    def test_encode_datetime(self):
        base_value = datetime(2016, 1, 5, 23, 30, 15)

        encoded = encode_datetime(base_value)
        assert isinstance(encoded, str)

        decoded = decode_datetime(encoded)
        assert decoded == base_value

    def test_encode_datetime_null(self):
        encoded = encode_datetime(None, allow_null=True)
        assert encoded is None

        decoded = decode_datetime(encoded, allow_null=True)
        assert decoded is None

    def test_encode_datetime_null_non_nullable(self):
        with pytest.raises(ValueError):
            encode_datetime(None, allow_null=False)

        with pytest.raises(ValueError):
            decode_datetime(None, allow_null=False)


class TestScheduleEncoding:
    @pytest.mark.parametrize('value_type,value', [
        ('datetime', datetime(2016, 1, 5, 23, 30, 15)),
        ('crontab', crontab('10', '*/2')),
        ('solar', solar('sunset', -37.81753, 144.96715)),
        ('schedule', schedule(17, relative=True)),
    ])
    def test_encode_decode(self, value_type, value):
        encoded = encode_schedule(value)
        assert encoded['__type__'] == value_type

        decoded = decode_schedule(encoded)
        assert decoded == value
