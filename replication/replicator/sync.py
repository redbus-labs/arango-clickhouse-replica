# noinspection PyPackageRequirements
import threading

# noinspection PyPackageRequirements
from kafka import KafkaAdminClient, KafkaConsumer
# noinspection PyPackageRequirements
from kafka.admin import NewTopic

from cache.connect import get_singleton_redis_client
from replication.consumer.task import Status
from replication.replicator.pm2 import PM2, get_config_path, generate_config_file
from replication.replicator.store import load_collection_data
from replication.schema.helper import get_table_map_by_arango_collection
from taskmanager import TaskManager
from util.basic_utils import get_basic_utilities, CONFIG, LOGGER


def delete_topics(topics, time_out=10):
    # noinspection PyBroadException
    config = get_basic_utilities().get(CONFIG)
    kafka_config = config['kafka']
    server = f"{kafka_config['host']}:{kafka_config['port']}"
    admin_client = KafkaAdminClient(bootstrap_servers=server)
    consumer = KafkaConsumer(bootstrap_servers=server)
    active_topics = [topic for topic in consumer.topics() if topic in topics]
    admin_client.delete_topics(topics=active_topics)
    stop = threading.Event()
    all_deleted = False

    def is_deleted():
        nonlocal all_deleted
        while not stop.is_set():
            current_topics = consumer.topics()
            for topic in active_topics:
                if topic in current_topics:
                    stop.wait(timeout=1)
                    break
            else:
                stop.set()
                all_deleted = True

    thread = threading.Thread(name='delete-topics', target=is_deleted)
    thread.start()
    stop.wait(timeout=time_out)
    stop.set()
    thread.join()
    return all_deleted


def create_topic(table):
    config, logging = get_basic_utilities().get_utils((CONFIG, LOGGER))
    table_map = get_table_map_by_arango_collection(table)
    if not table_map:
        return False
    kafka_config = config['kafka']
    admin_client = KafkaAdminClient(
        bootstrap_servers=f"{kafka_config['host']}:{kafka_config['port']}",
    )

    # create kafka topic
    custom_topic_configs = table_map['topic_config'] if 'topic_config' in table_map else {}
    topic_config = {
        'name': table,
        'num_partitions': 1,
        'replication_factor': 1,
        'topic_configs': custom_topic_configs
    }
    new_topic = NewTopic(**topic_config)
    admin_client.create_topics([new_topic])
    logging.info(f'{table} topic created')

    return True


def synchronizer(tables, clear):
    config, logging = get_basic_utilities().get_utils((CONFIG, LOGGER))
    redis_config = config['redis']
    redis_helper = get_singleton_redis_client(redis_config['host'], redis_config['port'], redis_config['db'])

    try:
        generate_config_file()
    except (FileNotFoundError, Exception) as e:
        logging.error(f'unable to generate pm2 config file: {e}', exc_info=True)
        return False

    pm2_config_path = get_config_path()
    producer_process = PM2('arango-producer', pm2_config_path)
    consumer_process = PM2('clickhouse-consumer', pm2_config_path)
    task_manager = TaskManager(redis_helper)

    # clear redis cache db if specified
    if clear:
        redis_helper.client.flushdb()
        logging.info('redis cache cleared')
    else:
        # delete consumer specific keys
        for table in tables:
            for key in redis_helper.client.keys(f'{table}*'):
                redis_helper.client.delete(key)

    # stop the producer process
    if not producer_process.stop():
        logging.error('unable to stop producer')
        return False

    # stop the consumer process
    for table in tables:
        consumer_active = task_manager.ping(table)
        if consumer_active:
            result = task_manager.stop_task(table)
            if result == Status.INACTIVE.name:
                logging.info(f'stopped the consumer {table}')
            else:
                logging.error(f'unable to stop consumer {table}')
                return False
        else:
            logging.info(f'consumer {table} not active')

    # delete topics
    all_deleted = delete_topics(tables)
    if not all_deleted:
        logging.error(f'unable to delete all kafka topics')
        return False

    # create topic
    for table in tables:
        created = create_topic(table)
        if not created:
            logging.error(f'unable to delete topic: {table}')
            return False

    # start producer process
    if not producer_process.start():
        logging.error('unable to start producer')
        return False

    # sync existing collection data
    for table in tables:
        is_data_loaded = load_collection_data(collection=table, store_tick=True, batch_size=100000)
        if is_data_loaded:
            logging.info('existing data loaded to clickhouse')
        else:
            logging.error(f'failed to load {table} data')
            return False

        # start the consumer
        if task_manager.ping(table):
            result = task_manager.start_task(table)
            if result == Status.ACTIVE.name:
                logging.info(f'{table} consumer process started')
            else:
                logging.error('unable to start consumer, restarting using pm2')

    if consumer_process.restart():
        logging.info('pm2 consumer restarted')
    else:
        logging.error('unable to restart pm2 consumers')
        return False

    return True
