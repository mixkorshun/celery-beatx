from urllib.parse import urlparse

from celery.beat import Scheduler as BaseScheduler
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

from .utils import import_string

logger = get_logger(__name__)


class Scheduler(BaseScheduler):
    """
    Celery scheduler which use store class to load/save schedule.

    Only single instance of running beat instances will be active.
    Another instances will run in "sleep-mode" and will waiting
    when master instance will dead[or not :-)].
    """

    @staticmethod
    def get_store(app):
        """
        Get store object from celery application.

        :param app: celery application
        :return: store
        """
        store_classes = getattr(app.conf, 'beatx_store_classes', {
            'dummy': 'beatx.store.dummy.Store',
            'redis': 'beatx.store.redis.Store',
            'memcached': 'beatx.store.memcached.MemcachedStore',
            'pylibmc': 'beatx.store.memcached.PyLibMCStore',
        })
        store_url = getattr(app.conf, 'beatx_store')

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

        super().__init__(app, *args, **kwargs)

        self.lock_ttl = getattr(
            app.conf,
            'beatx_store_lock_ttl',
            self.max_interval + 1
        )

        if self.max_interval >= self.lock_ttl:
            raise ImproperlyConfigured(
                '`beatx_store_lock_ttl` must be greater '
                'then `beat_max_loop_interval`'
            )

    def setup_schedule(self):
        if self.store.has_locked():
            self.install_default_entries(self.schedule)
            self.update_from_dict(self.store.load_entries())
            self.merge_inplace(self.app.conf.beat_schedule)

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

        return super().tick(*args, **kwargs)

    def close(self):
        super().close()

        if self.store.has_locked():
            self.store.release_lock()
            logger.info('beatX: Lock released.')
