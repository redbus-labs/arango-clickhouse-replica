# noinspection PyPackageRequirements
from kafka import KafkaAdminClient

from util.basic_utils import CONFIG
# noinspection PyUnresolvedReferences
from .test_basic_utils import basic_utilities


def test_kafka_connection(basic_utilities):
    config = basic_utilities.get(CONFIG)
    kafka_config = config['kafka']
    server = f"{kafka_config['host']}:{kafka_config['port']}"
    KafkaAdminClient(bootstrap_servers=server)
    return True
