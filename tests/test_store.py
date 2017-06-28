from datetime import datetime
from os import environ

from celery.beat import ScheduleEntry

from beatx.store import dummy, redis


def test_dummy_store_api():
    store = dummy.Store('dummy://')

    assert store.load_entries() == {}

    store.save_entries({})

    assert store.acquire_lock(60) is True
    assert store.has_locked() is True
    store.renew_lock()
    store.release_lock()


class TestRedisStore:
    store = None

    def setup_method(self):
        self.store = redis.Store(
            environ.get('REDIS_URL', 'redis://127.0.0.1/0')
        )

    def teardown_method(self):
        self.store.rdb.flushdb()

    def test_entries_save_load(self):
        entries = {
            'entry-1': ScheduleEntry(
                name='entry-1',
                last_run_at=datetime.utcnow()
            ),
        }

        self.store.save_entries(entries)

        loaded = self.store.load_entries()

        assert entries.keys() == loaded.keys()

    def test_load_entries_from_empty_db(self):
        assert self.store.load_entries() == {}

    def test_acquire_new_lock(self):
        acquired = self.store.acquire_lock(60)

        assert acquired is True
        assert self.store.has_locked() is True

        assert self.store.rdb.exists(self.store.LOCK_KEY) is True

    def test_acquire_exists_lock(self):
        self.store.rdb.lock(
            self.store.LOCK_KEY, timeout=60, sleep=1
        ).acquire(blocking=False)

        acquired = self.store.acquire_lock(60)

        assert acquired is False
        assert self.store.has_locked() is False

    def test_renew_lock(self):
        self.store.acquire_lock(60)
        assert self.store.rdb.pexpire(self.store.LOCK_KEY, 15000)

        self.store.renew_lock()
        assert 59 <= self.store.rdb.ttl(self.store.LOCK_KEY) <= 60

    def test_release_lock(self):
        self.store.acquire_lock(60)

        self.store.release_lock()
        assert self.store.has_locked() is False

        assert self.store.rdb.exists(self.store.LOCK_KEY) is False
