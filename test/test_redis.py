from cache.connect import get_singleton_redis_client
from util.basic_utils import CONFIG, get_basic_utilities
# noinspection PyUnresolvedReferences
from .test_basic_utils import basic_utilities


def get_redis_client():
    config = get_basic_utilities().get(CONFIG)
    redis_helper = get_singleton_redis_client(config['redis']['host'], config['redis']['port'], config['redis']['db'])
    return redis_helper


def test_clickhouse_connection(basic_utilities):
    redis_helper = get_redis_client()
    return redis_helper.is_connected()
