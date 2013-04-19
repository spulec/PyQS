import fnmatch
import importlib
from multiprocessing import Queue

import boto

from pyqs.utils import decode_message

internal_queue = Queue()

conn = None


def get_conn():
    # TODO clean this up
    global conn
    if conn:
        return conn
    else:
        conn = boto.connect_sqs()
        return conn


class ReadWorker(object):

    def __init__(self, queue):
        self.queue = queue

    def read_queue(self):
        message = self.queue.read()
        message_body = decode_message(message)
        message.delete()

        internal_queue.put(message_body)


class ProcessWorker(object):
    def process_messages(self):
        next_message = internal_queue.get()

        task_path = next_message['task']
        args = next_message['args']
        kwargs = next_message['kwargs']

        task_name = task_path.split(".")[-1]
        task_path = ".".join(task_path.split(".")[:-1])

        task_module = importlib.import_module(task_path)

        task = getattr(task_module, task_name)
        task(*args, **kwargs)


class ManagerWorker(object):

    def __init__(self, queue_prefix, worker_concurrency=1):
        self.queue_prefix = queue_prefix
        self.queues = self.get_queues_from_queue_prefix(self.queue_prefix)
        self.reader_children = []
        self.worker_children = []

        for queue in self.queues:
            self.reader_children.append(ReadWorker(queue))

        for index in range(worker_concurrency):
            self.worker_children.append(ProcessWorker())

    def get_queues_from_queue_prefix(self, queue_prefix):
        all_queues = get_conn().get_all_queues()
        return [
            queue for queue in all_queues if
            fnmatch.fnmatch(queue.name, queue_prefix)
        ]
