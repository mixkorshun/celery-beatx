import json

from redis import StrictRedis

from beatx import serializer


class Store:
    SCHEDULE_KEY = 'celery:schedule'
    LOCK_KEY = 'celery:beat-lock'

    def __init__(self, store):
        self.rdb = StrictRedis.from_url(store)
        self.lock = None

    def load_entries(self):
        return {
            name.decode(): serializer.deserialize_entry(
                json.loads(data.decode())
            ) for name, data in self.rdb.hgetall(self.SCHEDULE_KEY).items()
        }

    def save_entries(self, entries):
        self.rdb.delete(self.SCHEDULE_KEY)
        if entries:
            self.rdb.hmset(self.SCHEDULE_KEY, {
                name: json.dumps(
                    serializer.serialize_entry(entry)
                ) for name, entry in entries.items()
            })

    def has_locked(self):
        return self.lock is not None

    def acquire_lock(self, lock_ttl=60):
        lock = self.rdb.lock(self.LOCK_KEY, timeout=lock_ttl, sleep=1)

        if lock.acquire(blocking=False):
            self.lock = lock
            return True

        return False

    def renew_lock(self):
        if not self.has_locked():
            raise RuntimeError("Cannot renew lock: lock is not set.")

        self.rdb.pexpire(self.LOCK_KEY, self.lock.timeout * 1000)

    def release_lock(self):
        self.lock.release()
        self.lock = None
