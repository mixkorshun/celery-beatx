from unittest.mock import Mock


def get_mock_app(store_url='mock://'):
    return Mock(
        conf=Mock(
            CELERY_BEAT_SCHEDULE={},
            CELERY_BEAT_STORE=store_url,
            CELERY_BEAT_STORE_LOCK_TTL=60,

            CELERY_BEAT_STORE_CLASSES={
                'dummy': 'beatx.store.dummy.Store',
                'redis': 'beatx.store.redis.Store',
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
        )
    )
