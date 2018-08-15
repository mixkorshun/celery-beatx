import pytest
from celery.exceptions import ImproperlyConfigured
from unittest.mock import Mock, patch

from beatx.schedulers import BeatXScheduler
from .utils import get_mock_app


def test_get_unknown_store():
    with pytest.raises(ImproperlyConfigured):
        BeatXScheduler.get_store(get_mock_app('unknown://'))


class TestScheduler:
    def test_load_entries_on_setup(self):
        schedule = BeatXScheduler(get_mock_app())

        schedule.store = Mock(load_entries=Mock(return_value={}))
        schedule.setup_schedule()

        assert schedule.store.load_entries.called

    def test_save_entries_on_sync(self):
        schedule = BeatXScheduler(get_mock_app())

        schedule.store = Mock()

        schedule.sync()
        assert schedule.store.save_entries.called


class TestClusterSchedulerActiveMode:
    """
    Test active mode of cluster-scheduler.
    """

    def test_entries_sync_on_setup(self):
        scheduler = BeatXScheduler(get_mock_app())

        assert scheduler.store.load_entries.called is True
        assert scheduler.store.save_entries.called is True

    def test_lock_renew_on_tick(self):
        scheduler = BeatXScheduler(get_mock_app())

        scheduler.tick()

        assert scheduler.store.renew_lock.called is True

    @patch('beatx.schedulers.BeatXScheduler.tick')
    def test_tick_working(self, base_tick):
        scheduler = BeatXScheduler(get_mock_app())

        scheduler.tick()

        assert base_tick.called is True

    def test_sync_active(self):
        scheduler = BeatXScheduler(get_mock_app())

        scheduler.sync()

        assert scheduler.store.save_entries.called is True

    def test_release_lock_on_close(self):
        scheduler = BeatXScheduler(get_mock_app())

        scheduler.close()

        assert scheduler.store.release_lock.called is True


class TestClusterSchedulerInactiveMode:
    """
    Test inactive mode of cluster-scheduler.
    """

    def test_setup(self):
        scheduler = BeatXScheduler(get_mock_app('mock+inactive://'))

        assert scheduler.store.load_entries.called is False
        assert scheduler.store.save_entries.called is False

    @patch('beatx.schedulers.Scheduler.tick')
    def test_inactive_tick(self, base_tick):
        scheduler = BeatXScheduler(get_mock_app('mock+inactive://'))

        ret_val = scheduler.tick()

        assert base_tick.called is False
        assert ret_val == scheduler.max_interval

    @patch('beatx.schedulers.Scheduler.tick')
    def test_acquire_lock_on_tick(self, base_tick):
        scheduler = BeatXScheduler(get_mock_app('mock+inactive://'))

        scheduler.store.acquire_lock.return_value = True

        scheduler.tick()

        assert scheduler.store.acquire_lock.called is True
        assert base_tick.called is True

    def test_sync_not_write(self):
        scheduler = BeatXScheduler(get_mock_app('mock+inactive://'))

        scheduler.sync()

        assert scheduler.store.save_entries.called is False
