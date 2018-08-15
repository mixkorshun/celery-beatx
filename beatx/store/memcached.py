try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse
from beatx import serializer


class BaseStore:
    SCHEDULE_KEY = 'celery:schedule'
    LOCK_KEY = 'celery:beat-lock'

    def __init__(self, store):
        self.client = self.get_client_from_url(store)
        self.lock = None

    @classmethod
    def get_client_from_url(cls, url):
        raise NotImplementedError()

    def load_entries(self):
        return {
            name: serializer.deserialize_entry(data)
            for name, data in self.client.get(self.SCHEDULE_KEY, {}).items()
        }

    def save_entries(self, entries):
        self.client.set(self.SCHEDULE_KEY, {
            name: serializer.serialize_entry(entry)
            for name, entry in entries.items()
        })

    def has_locked(self):
        return self.lock is not None

    def acquire_lock(self, lock_ttl=60):
        is_locked = self.client.add(self.LOCK_KEY, 'lock', lock_ttl)

        if is_locked:
            self.lock = ('lock', lock_ttl)

        return is_locked

    def renew_lock(self):
        if not self.has_locked():
            raise RuntimeError("Cannot renew lock: lock is not set.")

        lock_value, lock_ttl = self.lock

        self.client.set(self.LOCK_KEY, lock_value, lock_ttl)

    def release_lock(self):
        self.client.delete(self.LOCK_KEY)
        self.lock = None


class MemcachedStore(BaseStore):
    @classmethod
    def get_client_from_url(cls, url):
        from memcache import Client

        url_p = urlparse(url)
        servers = url_p.netloc.split(',')

        return Client(servers)


class PyLibMCStore(BaseStore):
    @classmethod
    def get_client_from_url(cls, url):
        from pylibmc import Client

        url_p = urlparse(url)
        servers = url_p.netloc.split(',')

        return Client(servers)
