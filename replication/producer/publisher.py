import os
import pathlib
import threading
import traceback

from arangodb.connect import get_singleton_arango_client
from arangodb.wal import ArangoWAL
from cache.connect import RedisHelper, get_singleton_redis_client
from replication.producer.reader import get_logs, get_wal_client, LogOpTypes, json_encode, key_encode, LogGenerator
from replication.producer.writer import get_log_writer
from util.basic_utils import LOGGER, CONFIG, SMTP_CLIENT, prepare_logger
from util.basic_utils import get_basic_utilities
from util.terminate import Terminate


def get_logger():
    config, logging = get_basic_utilities().get_utils((CONFIG, LOGGER))
    if 'logs_path' in config['logs']:
        logs_path = str(pathlib.Path(config['logs']['logs_path']).joinpath('producer'))
        return prepare_logger(logs_path, os.getenv('env'))
    return logging


def get_last_processed_tick(redis_helper: RedisHelper):
    last_tick = redis_helper.client.get("last-tick")
    last_tick = int(last_tick) if last_tick else last_tick
    return last_tick


def set_tick_if_not_set(wal_client: ArangoWAL, redis_helper):
    last_tick = get_last_processed_tick(redis_helper)
    if last_tick is not None:
        return None
    tick = wal_client.get_last_tick()['tick']
    updated_last_processed_tick(redis_helper, tick)
    return tick


def updated_last_processed_tick(redis_helper: RedisHelper, tick):
    return redis_helper.client.set("last-tick", tick)


def update_file_last_tick(file, tick):
    file.seek(0)
    file.write(f'{tick};')
    file.flush()


def get_arango_collections(arango_config):
    arango_db_client = get_singleton_arango_client(arango_config)
    return arango_db_client.db.collections


def is_document_allowed(document, collections_ids):
    document_cuid = document['cuid'] if 'cuid' in document else None
    document_type = document['type']
    return ((document_type == LogOpTypes.UPSERT_DOCUMENT or document_type == LogOpTypes.REMOVE_DOCUMENT) and
            document_cuid in collections_ids)


# result batch size may be vary due to arango log chunk_size and log filtering
def collect_logs(arango_wal_client, tick_min, batch_size, collections):
    collections_id_set = set(collections.values())
    log_generator = LogGenerator(get_logs(arango_wal_client, tick_min, batch_size))
    for documents in log_generator:
        documents['content'] = [document for document in documents['content'] if
                                is_document_allowed(document, collections_id_set)]
        is_processed = yield documents
        log_generator.is_processed(is_processed)


def get_id_collection_map(collection_id_dict):
    return {value: key for key, value in collection_id_dict.items()}


def get_topic_name(document, collection_id_dict):
    return collection_id_dict[document['cuid']]


def prepare_kafka_documents(collections_id_dict, docs, writer_timeout):
    kafka_documents = []
    for doc in docs['content']:
        kafka_document = {'topic': get_topic_name(doc, collections_id_dict), 'value': doc,
                          'timeout': writer_timeout}
        try:
            kafka_document['key'] = doc['data']['_key']
        except KeyError:
            kafka_document['key'] = None
        kafka_documents.append(kafka_document)
    return kafka_documents


def on_producer_exit():
    logging = get_logger()
    logging.info('producer terminating gracefully')


def producer():
    utils = get_basic_utilities()
    config, alert = utils.get_utils((CONFIG, SMTP_CLIENT))
    logging = get_logger()
    producer_config = config['producer']

    arango_wal_client = get_wal_client({**config['arango'], **config['wal']})
    redis_helper = get_singleton_redis_client(config['redis']['host'], config['redis']['port'], config['redis']['db'])

    last_tick_file = open('last-tick.txt', 'w')

    arango_collections = get_arango_collections(config['arango'])
    collections_id_dict = {collection: meta.globallyUniqueId for collection, meta in arango_collections.items() if
                           collection in producer_config['sync']}
    id_to_collection_dict = get_id_collection_map(collections_id_dict)

    logging.info(f'listening collections: {list(collections_id_dict.keys())}')

    # set last-tick as first tick during only the first start
    init_tick = set_tick_if_not_set(arango_wal_client, redis_helper)
    if init_tick:
        logging.info(f'stored initial tick: {init_tick}')

    _ = config['producer']['reader_batch']
    writer_timeout = config['producer']['writer_timeout']

    log_writer = get_log_writer(config['kafka']['host'], config['kafka']['port'], key_encode, json_encode)()

    exit_event = threading.Event()
    _ = Terminate(exit_event)

    while not exit_event.is_set():
        last_tick = get_last_processed_tick(redis_helper)
        logging.info(f'last processed tick: {last_tick}')

        logs_collector = collect_logs(arango_wal_client, last_tick, None, collections_id_dict)
        logs_generator = LogGenerator(logs_collector)

        for docs in logs_generator:

            tick_start = docs['content'][0]['tick'] if len(docs['content']) > 0 else None

            if not docs['from_present']:
                logging.error(f'ticks lost asked for {last_tick} but got {tick_start}')

            # store in kafka
            log_writer.bulk_write(prepare_kafka_documents(id_to_collection_dict, docs, writer_timeout))
            log_writer.flush()

            # update tick only if valid
            if int(docs['last_included']) > 0:
                if updated_last_processed_tick(redis_helper, docs['last_included']):
                    update_file_last_tick(last_tick_file, docs['last_included'])
                    # if is_processed set to False then the data batch will be processed again
                    # setting False always will lead to infinite loop
                    logs_generator.is_processed(True)

            logging.info(f'processed {f"{tick_start}-" if tick_start else ""}{docs["last_included"]}: '
                         f'overall {len(docs["content"])} docs')

            # handle termination call
            if exit_event.is_set():
                break

        logging.info('sleeping')
        exit_event.wait(timeout=config['producer']['idle'])

    logging.info('producer terminated gracefully')


def main():
    utils = get_basic_utilities()
    config, alert = utils.get_utils((CONFIG, SMTP_CLIENT))
    logging = get_logger()
    alert_config = config['alert']
    logging.info('log-producer started')
    # noinspection PyBroadException
    try:
        producer()
    except Exception:
        logging.error(f"producer failed: {traceback.format_exc()}")
        alert.send(alert_config["sender"], alert_config["receivers"], "CH-Sync: Producer failed")


if __name__ == '__main__':
    main()
