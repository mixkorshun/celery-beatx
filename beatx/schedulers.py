try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import sys

from celery.beat import Scheduler
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

from .utils import import_string

PY2 = sys.version_info[0] == 2

logger = get_logger(__name__)


class BeatXScheduler(Scheduler):
    """
    Celery scheduler which use store class to load/save schedule.

    Only single instance of running beat instances will be active.
    Another instances will run in "sleep-mode" and will waiting
    when master instance will dead[or not :-)].
    """
    classes_config = 'beatx_store_classes'
    store_config = 'beatx_store'
    store_lock_ttl_config = 'beatx_store_lock_ttl'

    @staticmethod
    def get_store_classes(app, config_key):
        store_classes = getattr(app.conf, config_key, {
            'dummy': 'beatx.store.dummy.Store',
            'redis': 'beatx.store.redis_store.Store',
            'memcached': 'beatx.store.memcached.MemcachedStore',
            'pylibmc': 'beatx.store.memcached.PyLibMCStore',
        })
        return store_classes

    @staticmethod
    def get_store(app):
        """
        Get store object from celery application.

        :param app: celery application
        :return: store
        """
        has_upper_classes_config = hasattr(
            app.conf,
            BeatXScheduler.classes_config.upper()
        )
        if has_upper_classes_config:
            store_classes = BeatXScheduler.get_store_classes(
                app, BeatXScheduler.classes_config.upper()
            )
        else:
            store_classes = BeatXScheduler.get_store_classes(
                app, BeatXScheduler.classes_config
            )

        has_upper_store_config = hasattr(
            app.conf,
            BeatXScheduler.store_config.upper()
        )
        if has_upper_store_config:
            store_url = getattr(app.conf, BeatXScheduler.store_config.upper())
        else:
            store_url = getattr(app.conf, BeatXScheduler.store_config)

        scheme = urlparse(store_url).scheme

        try:
            store_class = store_classes[scheme]
        except KeyError:
            raise ImproperlyConfigured(
                '"%(used)s" store scheme is unsupported. '
                'Available stores schemes: %(available)s' % {
                    'used': scheme,
                    'available': ', '.join(
                        '"%s"' % x for x in store_classes.keys()
                    )
                }
            )

        if not isinstance(store_class, type):
            store_class = import_string(store_class)

        return store_class(store_url)

    def __init__(self, app, *args, **kwargs):
        self.store = self.get_store(app)

        if not PY2:
            super().__init__(app, *args, **kwargs)
        else:
            super(BeatXScheduler, self).__init__(app, *args, **kwargs)

        has_upper = hasattr(
            app.conf,
            BeatXScheduler.store_lock_ttl_config.upper()
        )
        if has_upper:
            self.lock_ttl = getattr(
                app.conf,
                BeatXScheduler.store_lock_ttl_config.upper(),
                self.max_interval + 1
            )
        else:
            self.lock_ttl = getattr(
                app.conf,
                BeatXScheduler.store_lock_ttl_config,
                self.max_interval + 1
            )

        if self.max_interval >= self.lock_ttl:
            raise ImproperlyConfigured(
                '`beatx_store_lock_ttl` must be greater '
                'then `beat_max_loop_interval`'
            )

    def setup_schedule(self):
        if self.store.has_locked():
            self.merge_inplace(self.app.conf.beat_schedule)
            self.install_default_entries(self.schedule)
            self.update_from_dict(self.store.load_entries())

            self.sync()

    def sync(self):
        if not self.store.has_locked():
            return

        self.store.save_entries(self.schedule)

    def acquire_lock(self):
        acquired = self.store.acquire_lock(self.lock_ttl)

        if acquired:
            self.setup_schedule()

        return acquired

    def tick(self, *args, **kwargs):
        if not self.store.has_locked():
            acquired = self.acquire_lock()

            if acquired:
                logger.info('beatX: Lock acquired.')
            else:
                logger.info('beatX: Awaiting lock...')
                return self.max_interval
        else:
            self.store.renew_lock()
            logger.info('beatX: Lock renewed.')

        if not PY2:
            return super().tick(*args, **kwargs)
        else:
            return super(BeatXScheduler, self).tick(*args, **kwargs)

    def close(self):

        if not PY2:
            super().close()
        else:
            super(BeatXScheduler, self).close()

        if self.store.has_locked():
            self.store.release_lock()
            logger.info('beatX: Lock released.')
