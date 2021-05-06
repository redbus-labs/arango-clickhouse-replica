import signal
import threading
import typing


class Terminate:

    def __init__(self, event: threading.Event):
        self.signum = None
        self.frame = None
        self.event = event
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.signum = signum
        self.frame = frame
        self.event.set()


class Thread(threading.Thread):

    def __init__(self, target: typing.Callable, name, args):
        threading.Thread.__init__(self)
        self.stop_thread = threading.Event()
        self.target = target
        self.name = name
        self.args = args

    def run(self):
        self.target(*self.args, self.stop_thread)
