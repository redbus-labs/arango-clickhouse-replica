import json
import threading
import time
import traceback
import typing
from datetime import datetime
from enum import Enum
from typing import Optional

from redis.client import PubSubWorkerThread

from cache.connect import RedisHelper


class Status(Enum):
    NOT_STARTED = 0
    ACTIVE = 1
    INACTIVE = 2
    RESTARTING = 3
    ERROR = 4
    COMPLETE = 5
    TERMINATE = 6


class Thread(threading.Thread):

    def __init__(self, target: typing.Callable, name, args, kwargs, redis):
        threading.Thread.__init__(self)
        self.target = target
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.exc = None
        self.trace = None
        self.redis: RedisHelper = redis

    def run(self):
        try:
            self.target(*self.args, **self.kwargs)
            signal: threading.Event = list(self.args)[-1]
            if not signal.is_set():
                self.redis.publish(f'{self.name}:manager', Status.COMPLETE.name)
        except Exception as e:
            self.exc = e
            self.trace = traceback.format_exc()
            self.redis.publish(f'{self.name}:manager', Status.ERROR.name)


class Task:

    # FIXME too many arguments
    # noinspection PyRedundantParentheses
    def __init__(self, func, args, kwargs, name, err_call_back, term_call_back, max_restarts=3,
                 min_up_time=60, restart_delay=10, redis=None):
        self.task = func
        self.args = args
        self.kwargs = kwargs
        self.name = name
        self.signal = threading.Event()
        self.finish = threading.Event()
        self.terminate = threading.Event()
        self.max_restarts = max_restarts
        self.min_up_time = min_up_time
        self.restart_delay = restart_delay
        self.err_call_back = err_call_back
        self.term_call_back = term_call_back
        self.redis: RedisHelper = redis
        self.thread: Optional[Thread] = None
        self.subscriber: Optional[PubSubWorkerThread] = None
        self.last_failed = None
        self.failed_at = None
        self.number_of_restarts = 0
        self.current_number_of_restarts = 0
        self.status = Status.NOT_STARTED
        self.listen_for_message()

    def listen_for_message(self):
        self.subscriber = self.redis.subscribe({f'{self.name}:manager': self.on_message})

    def on_message(self, message):
        message = message['data'].decode()
        if message == Status.ACTIVE.name:
            self.start_again()
            self.redis.client.publish(f'{self.name}:task:start', self.status.name)
        elif message == Status.INACTIVE.name:
            self.stop()
            self.redis.client.publish(f'{self.name}:task:stop', self.status.name)
        elif message == Status.RESTARTING.name:
            self.restart()
            self.redis.client.publish(f'{self.name}:task:restart', self.status.name)
        elif message == Status.COMPLETE.name:
            self.task_finish()
            self.redis.client.publish(f'{self.name}:task:finish', self.status.name)
        elif message == Status.ERROR.name:
            self.on_task_error()
        elif message == 'PING':
            self.redis.client.publish(f'{self.name}:task:ping', 'OK')
        elif message == 'INFO':
            last_failed = str(self.last_failed.strftime('%Y-%m-%d %H:%M:%S')) if self.last_failed else ''
            self.redis.client.publish(f'{self.name}:task:info', json.dumps({
                'status': self.status.name,
                'last_failed': last_failed,
                'number_of_restarts': self.number_of_restarts,
                'current_number_of_restarts': self.current_number_of_restarts,
                'max_restarts': self.max_restarts,
                'min_up_time': self.min_up_time
            }))

    def update_status(self, status):
        self.status = status
        self.redis.client.set(f'{self.name}:status', status.name)

    def reset_state(self):
        self.failed_at = None
        self.current_number_of_restarts = 0

    def set_current_reset_count(self, count):
        self.current_number_of_restarts = count

    def get_number_of_failures(self):
        return self.number_of_restarts

    def is_failed_again(self):
        now = time.time()
        if self.failed_at is None:
            self.failed_at = now
            return False
        diff = now - self.failed_at
        self.failed_at = now
        return diff < self.min_up_time

    def is_max_restart_limit_reached(self):
        return self.current_number_of_restarts < self.max_restarts

    def handle_failure(self, e):
        self.number_of_restarts = self.number_of_restarts + 1
        self.last_failed = datetime.now()
        failed_again = self.is_failed_again()
        if failed_again:
            self.set_current_reset_count(self.current_number_of_restarts + 1)
        else:
            self.set_current_reset_count(1)
        if self.err_call_back:
            self.err_call_back(self, e, self.thread.trace)
        is_restart_allowed = self.is_max_restart_limit_reached()
        if is_restart_allowed:
            time.sleep(self.restart_delay)
            self.start_again()
        else:
            self.status = Status.INACTIVE
            self.task_finish()
            self.term_call_back(self)

    def on_task_error(self):
        self.update_status(Status.RESTARTING)
        self.handle_failure(self.thread.exc)

    def task_finish(self):
        self.close_subscriber()
        self.update_status(Status.COMPLETE)
        self.finish.set()

    def terminate_task(self):
        self.close_subscriber()
        self.update_status(Status.TERMINATE)
        self.signal.set()
        self.terminate.set()

    def is_active(self):
        return self.status == Status.ACTIVE

    def close_subscriber(self):
        if self.subscriber and self.subscriber.is_alive():
            self.subscriber.stop()

    def start_again(self):
        if not self.terminate.is_set():
            self.signal.clear()
            self.start()

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = Thread(target=self.task, name=self.name, args=(*self.args, self.signal),
                             kwargs=self.kwargs, redis=self.redis)
        self.thread.start()
        self.update_status(Status.ACTIVE)

    def stop(self):
        self.signal.set()
        if self.thread.is_alive():
            self.thread.join()
        self.update_status(Status.INACTIVE)

    def restart(self):
        self.stop()
        self.update_status(Status.RESTARTING)
        self.reset_state()
        self.start_again()
