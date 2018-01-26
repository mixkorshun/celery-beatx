from urllib.parse import urlparse

from celery.beat import Scheduler as BaseScheduler
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

from .utils import import_string

logger = get_logger(__name__)


def get_store(app):
    """
    Get store object fro celery application.

    :param app: celery application
    :return: store
    """
    store_classes = getattr(app.conf, 'beat_store_classes', {
        'dummy': 'beatx.store.dummy.Store',
        'redis': 'beatx.store.redis.Store',
    })
    store_url = getattr(app.conf, 'beat_store')

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


class Scheduler(BaseScheduler):
    """
    Celery scheduler.

    Simple celery scheduler which use store class to load/save schedule.
    """

    def __init__(self, app, *args, **kwargs):
        self.store = get_store(app)

        super().__init__(app, *args, **kwargs)

    def setup_schedule(self):
        self.merge_inplace(self.app.conf.beat_schedule)
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.store.load_entries())

        self.sync()

    def sync(self):
        self.store.save_entries(self.schedule)


class ClusterScheduler(Scheduler):
    """
    Celery cluster-scheduler.

    Only single instance of running beat instances will be active.
    Another instances will run in "sleep-mode" and will waiting
    when master instance will dead[or not :-)].
    """

    def __init__(self, app, *args, **kwargs):
        super().__init__(app, *args, **kwargs)

        self.lock_ttl = getattr(
            app.conf,
            'beat_store_lock_ttl',
            self.max_interval + 1
        )

        if self.max_interval >= self.lock_ttl:
            raise ImproperlyConfigured(
                '`beat_store_lock_ttl` must be greater '
                'then `beat_max_loop_interval`'
            )

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
