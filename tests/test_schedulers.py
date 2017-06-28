from unittest.mock import Mock, patch

from beatx.schedulers import Scheduler, ClusterScheduler

mock_app = Mock(
    conf=Mock(
        CELERY_BEAT_SCHEDULE={},
        CELERY_BEAT_STORE='dummy://',
        CELERY_BEAT_STORE_LOCK_TTL=60
    )
)


class TestScheduler:
    def test_load_entries_on_setup(self):
        schedule = Scheduler(mock_app)

        schedule.store = Mock(load_entries=Mock(return_value={}))
        schedule.setup_schedule()

        assert schedule.store.load_entries.called

    def test_save_entries_on_sync(self):
        schedule = Scheduler(mock_app)

        schedule.store = Mock()

        schedule.sync()
        assert schedule.store.save_entries.called


class TestClusterSchedulerActiveMode:
    """
    Test active mode of multi-scheduler.
    """

    @classmethod
    def setup_class(cls):
        cls.__get_store_func = ClusterScheduler.get_store

        ClusterScheduler.get_store = Mock(
            return_value=Mock(
                acquire_lock=Mock(return_value=True),
                has_locked=Mock(return_value=True),
                load_entries=Mock(return_value={}),
            )
        )

    @classmethod
    def teardown_class(cls):
        ClusterScheduler.get_store = cls.__get_store_func

    def test_lock_acquired_on_setup(self):
        scheduler = ClusterScheduler(mock_app)

        assert scheduler.store.acquire_lock.called is True

    def test_entries_sync_on_setup(self):
        scheduler = ClusterScheduler(mock_app)

        assert scheduler.store.load_entries.called is True
        assert scheduler.store.save_entries.called is True

    def test_lock_renew_on_tick(self):
        scheduler = ClusterScheduler(mock_app)

        scheduler.tick()

        assert scheduler.store.renew_lock.called is True

    @patch('beatx.schedulers.BaseScheduler.tick')
    def test_tick_working(self, base_tick):
        scheduler = ClusterScheduler(mock_app)

        scheduler.tick()

        assert base_tick.called is True

    def test_sync_active(self):
        scheduler = ClusterScheduler(mock_app)

        scheduler.sync()

        assert scheduler.store.save_entries.called is True

    def test_release_lock_on_close(self):
        scheduler = ClusterScheduler(mock_app)

        scheduler.close()

        assert scheduler.store.release_lock.called is True


class TestClusterSchedulerInactiveMode:
    """
    Test inactive mode of multi-scheduler.
    """

    @classmethod
    def setup_class(cls):
        cls.__get_store_func = ClusterScheduler.get_store

        ClusterScheduler.get_store = Mock(
            return_value=Mock(
                acquire_lock=Mock(return_value=False),
                has_locked=Mock(return_value=False),
                load_entries=Mock(return_value={}),
            )
        )

    @classmethod
    def teardown_class(cls):
        ClusterScheduler.get_store = cls.__get_store_func

    def test_setup(self):
        scheduler = ClusterScheduler(mock_app)

        assert scheduler.store.acquire_lock.called is True
        assert scheduler.store.load_entries.called is False
        assert scheduler.store.save_entries.called is False

    @patch('beatx.schedulers.BaseScheduler.tick')
    def test_inactive_tick(self, base_tick):
        scheduler = ClusterScheduler(mock_app)

        ret_val = scheduler.tick()

        assert base_tick.called is False
        assert ret_val == scheduler.max_interval

    @patch('beatx.schedulers.BaseScheduler.tick')
    def test_acquire_lock_on_tick(self, base_tick):
        scheduler = ClusterScheduler(mock_app)

        scheduler.store.acquire_lock.return_value = True

        scheduler.tick()

        assert scheduler.store.acquire_lock.called is True
        assert base_tick.called is True

    def test_sync_not_write(self):
        scheduler = ClusterScheduler(mock_app)

        scheduler.sync()

        assert scheduler.store.save_entries.called is False
