from celery import Celery
from unittest.mock import Mock


def get_mock_app(store_url='mock://'):
    app = Celery('test_app')

    app.config_from_object({
        'beat_schedule': {},
        'beatx_store': store_url,

        'beatx_store_classes': {
            'dummy': 'beatx.store.dummy.Store',
            'redis': 'beatx.store.redis_store.Store',
            'mock': Mock(
                spec=type, return_value=Mock(
                    acquire_lock=Mock(return_value=True),
                    has_locked=Mock(return_value=True),
                    load_entries=Mock(return_value={}),
                )),
            'mock+inactive': Mock(
                spec=type, return_value=Mock(
                    acquire_lock=Mock(return_value=False),
                    has_locked=Mock(return_value=False),
                    load_entries=Mock(return_value={}),
                ))
        }
    })

    return app
