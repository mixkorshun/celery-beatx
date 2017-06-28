from unittest.mock import Mock

import pytest

from beatx.schedulers import Scheduler


def test_register_store():
    Scheduler.register_store_class('test_store', Mock)
    assert 'test_store' in Scheduler.store_registry

    Scheduler.unregister_store('test_store')
    assert 'test_store' not in Scheduler.store_registry


def test_get_store_instance():
    cls = Mock()

    Scheduler.register_store_class('test2', cls)

    Scheduler.get_store('test2://test')

    assert cls.called
    assert ('test2://test',) == cls.call_args[0]

    Scheduler.unregister_store('test2')


def test_get_unknown_store():
    with pytest.raises(KeyError):
        Scheduler.get_store('unknown://test')
