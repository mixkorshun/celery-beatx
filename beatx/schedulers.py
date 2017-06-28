from urllib.parse import urlparse

from celery.beat import Scheduler as BaseScheduler
from celery.utils.log import get_logger

logger = get_logger(__name__)


class Scheduler(BaseScheduler):
    """
    Celery scheduler.
    """
    store_registry = {}

    @classmethod
    def register_store_class(cls, scheme, store_class):
        """
        Register store scheme.

        :param scheme: store url scheme
        :param store_class: store class
        """
        cls.store_registry[scheme] = store_class

    @classmethod
    def unregister_store(cls, scheme):
        """
        Delete store scheme from registry.

        :param scheme: store url scheme
        """
        del cls.store_registry[scheme]

    @classmethod
    def get_store(cls, store_url):
        """
        Create store instance.

        :param store: store url
        :return: store instance
        """
        _url = urlparse(store_url)

        return cls.store_registry[_url.scheme](store_url)

    def __init__(self, app, *args, **kwargs):
        self.store = self.get_store(
            getattr(app.conf, 'CELERY_BEAT_STORE', 'dummy://')
        )
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

        if not self.acquire_lock():
            logger.info('beat: another beat instance already running. awaiting...')

    def acquire_lock(self):
        acquired = self.store.acquire_lock(self.lock_ttl)

        if acquired:
            logger.info('beat: lock acquired.')
            self.setup_schedule()

        return acquired

    def setup_schedule(self):
        if self.store.has_locked():
            super().setup_schedule()

    def tick(self, *args, **kwargs):
        if not self.store.has_locked():
            acquired = self.acquire_lock()

            if not acquired:
                return self.max_interval
        else:
            logger.debug('beat: renew lock.')
            self.store.renew_lock()

        return super().tick(*args, **kwargs)

    def sync(self):
        if not self.store.has_locked():
            return

        super().sync()

    def close(self):
        super().close()

        if self.store.has_locked():
            self.store.release_lock()
            logger.info('beat: lock released.')