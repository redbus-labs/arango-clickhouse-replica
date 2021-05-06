from typing import Optional

import pytest
from pyArango.collection import Collection

from arangodb.connect import get_singleton_arango_client
from replication.producer.reader import get_wal_client
from util.basic_utils import CONFIG, get_basic_utilities
# noinspection PyUnresolvedReferences
from .test_basic_utils import basic_utilities


def test_get_singleton_arango_client(basic_utilities):
    config = basic_utilities.get(CONFIG)
    arango_client = get_singleton_arango_client(config['arango'])
    new_arango_client = get_singleton_arango_client(config['arango'])
    return arango_client is new_arango_client


def test_arango_connection(basic_utilities):
    config = basic_utilities.get(CONFIG)
    get_singleton_arango_client(config['arango'])
    return True


def test_wal_connection(basic_utilities):
    config = basic_utilities.get(CONFIG)
    get_wal_client({**config['arango'], **config['wal']})
    return True


def get_test_table():
    config = get_basic_utilities().get(CONFIG)
    arango_client = get_singleton_arango_client(config['arango'])
    if 'test' in arango_client.db.collections:
        return arango_client.db.collections['test']
    return arango_client.db.createCollection(name='test')


@pytest.fixture
def temp_table(basic_utilities) -> Collection:
    return get_test_table()


@pytest.fixture
def temp_empty_table() -> Optional[Collection]:
    test_table = get_test_table()
    if test_table.truncate():
        return test_table
    return None
