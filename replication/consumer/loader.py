import os
import pathlib
import signal
import threading
import time
import traceback
from datetime import datetime

from cache.connect import RedisHelper, get_singleton_redis_client, get_redis_client
from clickhouse.connect import get_ch_client_with_dict_config
from replication.consumer.broker import custom_connect_consumer, all_messages_consumed
from replication.consumer.task import Task
from replication.consumer.transformer import convert_to_ch_dict_using_schema
from replication.producer.reader import LogOpTypes
from replication.schema.helper import get_table_map_by_arango_collection, create_buffer_table
from util.basic_utils import get_basic_utilities, CONFIG, LOGGER, SMTP_CLIENT, prepare_logger
from util.common import get_supported_consumers


def get_logger():
    config, logging = get_basic_utilities().get_utils((CONFIG, LOGGER))
    if 'logs_path' in config['logs']:
        logs_path = str(pathlib.Path(config['logs']['logs_path']).joinpath('consumer'))
        return prepare_logger(logs_path, os.getenv('env'))
    return logging


def get_initial_tick_of_consumer(redis_helper: RedisHelper, consumer_name):
    last_tick = f'{consumer_name}:last-tick'
    return redis_helper.client.get(last_tick)


def bulk_insert_documents(client, table, documents):
    if len(documents) > 0:
        columns = list(documents[0].keys())
        return client.bulk_dict_doc_insert(documents, table, columns)
    return 0


def transform_documents(schema, documents):
    clickhouse_documents = []
    errors = []
    for doc in documents:
        # noinspection PyBroadException
        try:
            clickhouse_documents.append(convert_to_ch_dict_using_schema(schema, doc))
        except Exception:
            errors.append((doc, traceback.format_exc()))
    return clickhouse_documents, errors


def filter_tombstone_messages(records):
    return [r for r in records if r['doc'] is not None]


def filter_relevant_docs(initial_tick, documents):
    filtered_documents = []
    for doc in documents:
        if int(doc['tick']) >= int(initial_tick):
            filtered_documents.append(doc)
    return filtered_documents


def pre_process_documents(initial_tick, records):
    records = filter_tombstone_messages(records)

    # pick only required attributes
    documents = [{**r['doc'], 'offset': r['offset']} for r in records]

    # filter documents that have tick < initial tick
    if initial_tick:
        documents = filter_relevant_docs(initial_tick, documents)

    # append document type
    for document in documents:
        document['data']['_ver'] = int(f"{datetime.utcnow().strftime('%Y%j')}{document['offset']}")
        document['data']['_deleted'] = 1 if document['type'] == LogOpTypes.REMOVE_DOCUMENT else 0

    return [doc['data'] for doc in documents]


def log_error_documents(error_documents):
    logging = get_logger()
    for error in error_documents:
        logging.document(f'doc: {error[0]}')
        logging.document(f'error: {error[1]}')


def data_consumer(consumer_name, stop_event: threading.Event):
    config = get_basic_utilities().get(CONFIG)
    logging = get_logger()

    logging.info(f'{consumer_name} started')

    # initialize necessary config
    kafka_config, consumer_config = config['kafka'], config['consumer']

    # initialize redis
    redis_config = config['redis']
    redis_helper = get_singleton_redis_client(redis_config['host'], redis_config['port'], redis_config['db'])
    initial_tick = get_initial_tick_of_consumer(redis_helper, consumer_name)

    # initialize clickhouse
    ch_client = get_ch_client_with_dict_config(config['clickhouse'])
    table_map = get_table_map_by_arango_collection(consumer_name)
    if table_map is None:
        logging.error('table map is not available')
        return False

    use_buffer = 'buffer' in table_map
    ch_table = f"{table_map['clickhouse']}_Buffer" if use_buffer else table_map['clickhouse']
    primary_key = table_map['schema']['primary_key']

    # create buffer table if not present
    if use_buffer:
        created = create_buffer_table(ch_client, table_map['clickhouse_db'], table_map['clickhouse'], table_map)
        if not created:
            logging.error('failed to create buffer table')
            return False

    # initialize kafka consumer
    consumer = custom_connect_consumer(kafka_config['host'], kafka_config['port'], consumer_name, consumer_name)
    idle, max_records, time_out = (consumer_config['idle'], consumer_config['kafka_max_records'],
                                   consumer_config['kafka_poll_time_out'])

    # noinspection PyBroadException
    try:
        while not stop_event.is_set():
            logging.info(f'{consumer_name}: polling for messages')
            msg_pack = consumer.poll(timeout_ms=time_out, max_records=max_records)

            for topic, messages in msg_pack.items():

                # _ = handle_messages([m.value for m in messages], consumer_name)
                documents = [{'offset': m.offset, 'doc': m.value} for m in messages]
                documents = pre_process_documents(initial_tick, documents)

                # set to none to skip initial tick validation
                if len(documents) > 0:
                    initial_tick = None

                # transform the documents
                documents, errors = transform_documents(table_map['schema'], documents)
                log_error_documents(errors)

                # insert the documents
                processed_count = bulk_insert_documents(ch_client, ch_table, documents)

                # log the documents
                for document in documents:
                    # logging.debug(f'{consumer_name}: message: {document}')
                    logging.info(f'{consumer_name}: processed: {document[primary_key]}, ver: {document["_ver"]}')

                logging.info(f'{consumer_name}: processed {processed_count} docs')

            # update offset in kafka
            consumer.commit()

            # idle the process
            is_messages_consumed = all_messages_consumed(consumer)
            if is_messages_consumed:
                logging.info(f'{consumer_name} process idle')
                stop_event.wait(timeout=idle)

    except Exception as e:
        consumer.close()
        raise e

    logging.info(f'{consumer_name} exited gracefully')


def on_consumer_failure(consumer_task: Task, e, trace):
    config, mail_client = get_basic_utilities().get_utils((CONFIG, SMTP_CLIENT))
    logging = get_logger()
    alert_config = config['alert']
    logging.error(f'{consumer_task.name} consumer failed: error {e}: {trace}')
    mail_client.send(alert_config["sender"], alert_config["receivers"],
                     f'CH-Sync: {consumer_task.name} consumer failed')


def on_consumer_terminate(consumer_task: Task):
    config, mail_client = get_basic_utilities().get_utils((CONFIG, SMTP_CLIENT))
    logging = get_logger()
    alert_config = config['alert']
    logging.error(f'{consumer_task.name} consumer terminated')
    mail_client.send(alert_config["sender"], alert_config["receivers"],
                     f'CH-Sync: {consumer_task.name} consumer terminated')


def exit_gracefully(consumer_tasks):
    for task in consumer_tasks:
        task.stop()
        task.terminate_task()


def check_tasks_completed(consumer_tasks: [Task], tasks_not_active: threading.Event, check_interval=10):
    def check():
        time.sleep(check_interval)
        while not tasks_not_active.is_set():
            for task in consumer_tasks:
                if not task.terminate.is_set() and not task.finish.is_set():
                    tasks_not_active.wait(timeout=check_interval)
                    break
            else:
                tasks_not_active.set()

    thread = threading.Thread(target=check, name='monitor-tasks')
    thread.start()


def main():
    utils = get_basic_utilities()
    config = utils.get(CONFIG)
    logging = get_logger()
    logging.info('starting the consumers')

    redis_config = config['redis']

    # start the supported consumers
    supported_consumers = get_supported_consumers()
    max_read_fails_allowed = config['consumer']['max_read_fails_allowed']
    min_up_time = config['consumer']['min_up_time']
    consumer_tasks = []
    for consumer in supported_consumers:
        task = Task(func=data_consumer, args=(consumer,), kwargs={}, name=consumer,
                    err_call_back=on_consumer_failure, term_call_back=on_consumer_terminate,
                    max_restarts=max_read_fails_allowed, min_up_time=min_up_time,
                    restart_delay=config['consumer']['restart_delay'],
                    redis=get_redis_client(redis_config['host'], redis_config['port'], redis_config['db']))
        task.start()
        consumer_tasks.append(task)

    # handle abnormal termination
    signal.signal(signal.SIGINT, lambda *x: exit_gracefully(consumer_tasks))
    signal.signal(signal.SIGTERM, lambda *x: exit_gracefully(consumer_tasks))

    # wait for all tasks to finish
    tasks_not_active = threading.Event()
    check_tasks_completed(consumer_tasks, tasks_not_active)
    tasks_not_active.wait()

    logging.info('consumers finished')


if __name__ == '__main__':
    main()
