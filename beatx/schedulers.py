from urllib.parse import urlparse

from celery.beat import Scheduler as BaseScheduler
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

from .utils import import_string

logger = get_logger(__name__)


def get_store(app):
    store_classes = getattr(app.conf, 'CELERY_BEAT_STORE_CLASSES', {
        'dummy': 'beatx.store.dummy.Store',
        'redis': 'beatx.store.redis.Store',
    })
    store_url = getattr(app.conf, 'CELERY_BEAT_STORE', 'dummy://')

    scheme = urlparse(store_url).scheme

    try:
        store_class = store_classes[scheme]
    except KeyError:
        raise ImproperlyConfigured(
            '"%(used)s" store scheme is unsupported. Available stores schemes: %(available)s' % {
                'used': scheme,
                'available': ', '.join('"%s"' % x for x in store_classes.keys())
            }
        )

    if not isinstance(store_class, type):
        store_class = import_string(store_class)

    return store_class(store_url)


class Scheduler(BaseScheduler):
    """
    Celery scheduler.
    """

    def __init__(self, app, *args, **kwargs):
        self.store = get_store(app)

        super().__init__(app, *args, **kwargs)

    def setup_schedule(self):
        self.merge_inplace(self.app.conf.CELERY_BEAT_SCHEDULE)
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.store.load_entries())

        self.sync()

    def sync(self):
        self.store.save_entries(self.schedule)


class ClusterScheduler(Scheduler):
    """
    Celery multi-scheduler.

    Only single instance of running beat instances will be active.
    Another instances will run in "sleep-mode" and will waiting
    when master instance will dead[or not :-)].
    """

    def __init__(self, app, *args, **kwargs):
        self.lock_ttl = getattr(app.conf, 'CELERY_BEAT_STORE_LOCK_TTL', 60)

        super().__init__(app, *args, **kwargs)

    def acquire_lock(self):
        acquired = self.store.acquire_lock(self.lock_ttl)

        if acquired:
            self.setup_schedule()

        return acquired

    def setup_schedule(self):
        if self.store.has_locked():
            super().setup_schedule()

    def tick(self, *args, **kwargs):
        if not self.store.has_locked():
            acquired = self.acquire_lock()

            if acquired:
                logger.info('beat: Lock acquired.')
            else:
                logger.info('beat: Awaiting lock...')
                return self.max_interval
        else:
            self.store.renew_lock()
            logger.debug('beat: Lock renewed.')

        return super().tick(*args, **kwargs)

    def sync(self):
        if not self.store.has_locked():
            return

        super().sync()

    def close(self):
        super().close()

        if self.store.has_locked():
            self.store.release_lock()
            logger.info('beat: Lock released.')
