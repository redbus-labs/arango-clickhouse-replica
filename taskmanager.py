import json
from typing import Optional

import click
from rich import box
from rich.console import Console
from rich.table import Table

from cache.connect import RedisHelper, get_singleton_redis_client
from replication.consumer.task import Status
from util.basic_utils import get_basic_utilities, CONFIG, LOGGER
from util.common import get_supported_consumers


class TaskManager:

    def __init__(self, redis):
        self.redis: RedisHelper = redis

    def message_consumer(self, from_channel, to_channel, option, interval=5) -> Optional[str]:

        def consume_message(message):
            nonlocal data, thread
            if message is not None:
                data = message['data'].decode() if message['data'] else None
                thread.stop()

        data = None
        pubsub = self.redis.client.pubsub()
        pubsub.subscribe(**{f'{from_channel}': consume_message})
        thread = pubsub.run_in_thread()
        self.redis.client.publish(to_channel, option)
        thread.join(interval)
        thread.stop()
        return data

    def ping(self, name, timeout=2):
        option = 'PING'
        message = self.message_consumer(f'{name}:task:{option.lower()}', f'{name}:manager', option, timeout)
        return message == 'OK'

    def get_task_info(self, name, timeout=3):
        option = 'INFO'
        message = self.message_consumer(f'{name}:task:{option.lower()}', f'{name}:manager', option, timeout)
        return message if message is None else json.loads(message)

    def get_task_status(self, name):
        return self.redis.client.get(f'{name}:status').decode()

    def start_task(self, name, timeout=10):
        option = Status.ACTIVE.name
        message = self.message_consumer(f'{name}:task:start', f'{name}:manager', option, timeout)
        return message

    def stop_task(self, name, timeout=60):
        option = Status.INACTIVE.name
        message = self.message_consumer(f'{name}:task:stop', f'{name}:manager', option, timeout)
        return message

    def restart_task(self, name, timeout=60):
        option = Status.RESTARTING.name
        message = self.message_consumer(f'{name}:task:restart', f'{name}:manager', option, timeout)
        return message

    def display_status(self, tasks):
        console = Console()
        table = Table(show_header=True, header_style="#00ccc8", box=box.ROUNDED)
        table.add_column("Name")
        table.add_column("Status")
        for task in tasks:
            status = self.get_task_status(task).lower()
            status = f'[bold #97ff6b]{status}[/bold #97ff6b]' \
                if status == Status.ACTIVE.name.lower() else f'[bold red]{status}[/bold red]'
            table.add_row(
                task,
                status
            )

        console.print(table)

    def display_info(self, tasks):
        console = Console()
        table = Table(show_header=True, header_style="#00ccc8", box=box.ROUNDED)
        table.add_column("Name")
        table.add_column("Status")
        table.add_column("Restarts")
        table.add_column("Last Failure")
        for task in tasks:
            info = self.get_task_info(task)
            if info is None:
                console.print(f'[bold red] unable to fetch {task} info [/bold red]')
                continue
            color = '#97ff6b' if info['status'] == Status.ACTIVE.name else 'red'
            table.add_row(
                task,
                f"[bold {color}]{info['status'].lower()}[/bold {color}]",
                str(info['number_of_restarts']),
                info['last_failed'] if info['last_failed'] else ''
            )

        if table.row_count > 0:
            console.print(table)


options_map = {
    Status.ACTIVE.name: TaskManager.start_task,
    Status.INACTIVE.name: TaskManager.stop_task,
    Status.RESTARTING.name: TaskManager.restart_task,
    'INFO': TaskManager.get_task_info,
    'STATUS': TaskManager.get_task_status
}

options_status_map = {
    Status.ACTIVE.name: 'start',
    Status.INACTIVE.name: 'stop',
    Status.RESTARTING.name: 'restart',
    'INFO': 'details',
    'STATUS': 'status'
}


def handle_consumers(consumers, option):
    console = Console()
    utils = get_basic_utilities()
    config, logging = utils.get_utils((CONFIG, LOGGER))
    redis_config = config['redis']
    redis_helper = get_singleton_redis_client(redis_config['host'], redis_config['port'], redis_config['db'])
    task_manager = TaskManager(redis_helper)
    operation = options_map[option]
    active_consumers = []
    for consumer in consumers:
        status = task_manager.get_task_info(consumer)
        if not status or status == Status.COMPLETE.name:
            console.print(f'[bold red] {consumer} not running[/bold red]')
        else:
            active_consumers.append(consumer)
    if len(active_consumers) < 1:
        return
    if option == 'INFO':
        task_manager.display_info(active_consumers)
    if option == 'STATUS':
        task_manager.display_status(active_consumers)
    if option == Status.ACTIVE.name or option == Status.INACTIVE.name or option == Status.RESTARTING.name:
        for consumer in active_consumers:
            # noinspection PyArgumentList
            _ = operation(task_manager, consumer)
        task_manager.display_status(active_consumers)
    return


@click.command()
@click.option('-status', is_flag=True, help='Status of the consumer')
@click.option('-start', is_flag=True, help='Start the consumer')
@click.option('-stop', is_flag=True, help='Stop the consumer')
@click.option('-restart', is_flag=True, help='Restart the consumer')
@click.option('-info', is_flag=True, help='Details of the consumer')
@click.option('-consumer', '-c', type=click.Choice(get_supported_consumers()), help='Name of consumer')
def main(status, start, stop, restart, info, consumer):
    console = Console()
    options = [(info, 'INFO'), (start, Status.ACTIVE.name), (stop, Status.INACTIVE.name),
               (restart, Status.RESTARTING.name), (status, 'STATUS')]
    options_provided = []
    for (is_set, option) in options:
        if is_set:
            options_provided.append(option)
    if len(options_provided) < 1:
        console.print('[bold red] no option provided [/bold red]')
        return False
    if len(options_provided) > 1:
        console.print('[bold red] too many options [/[bold red]]')
        return False
    option_set = options_provided[0]
    consumers = [consumer] if consumer else get_supported_consumers()
    handle_consumers(consumers, option_set)


if __name__ == '__main__':
    main()
