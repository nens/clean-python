from contextlib import contextmanager
from typing import Type
from unittest import mock

from .manage import Manage


@contextmanager
def mock_manage(manage_cls: Type[Manage], skip=()):
    """Mock all 'manage_' properties of a Manage class"""
    manager = manage_cls()

    mocks = {}
    for attr_name in dir(manage_cls):
        if not attr_name.startswith("manage_") or attr_name in skip:
            continue
        other_manager = getattr(manager, attr_name)
        if not isinstance(other_manager, Manage):
            continue
        mocks[attr_name] = mock.MagicMock(other_manager)

    patchers = [
        mock.patch.object(
            manage_cls,
            name,
            new_callable=mock.PropertyMock(return_value=x),
        )
        for name, x in mocks.items()
    ]
    for p in patchers:
        p.start()
    yield
    for p in patchers:
        p.stop()
