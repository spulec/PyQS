import json
import logging
from multiprocessing import Queue
from os.path import abspath, dirname, join, pardir
from Queue import Empty

import boto
from boto.sqs.message import Message
from moto import mock_sqs

from pyqs.worker import ReadWorker, ProcessWorker
from tests.tasks import task_results


@mock_sqs
def test_worker_fills_internal_queue():
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    message = Message()
    body = json.dumps({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })
    message.set_body(body)
    queue.write(message)

    internal_queue = Queue()
    worker = ReadWorker(queue, internal_queue)
    worker.read_message()

    found_message = internal_queue.get(timeout=1)

    found_message.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })


@mock_sqs
def test_worker_fills_internal_queue_only_until_maximum_queue_size():
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    message = Message()
    body = json.dumps({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })
    message.set_body(body)
    for i in range(3):
        queue.write(message)

    internal_queue = Queue(maxsize=2)
    worker = ReadWorker(queue, internal_queue)
    worker.read_message()

    # The internal queue should only have two messages on it
    internal_queue.get(timeout=1)
    internal_queue.get(timeout=1)

    try:
        internal_queue.get(timeout=1)
    except Empty:
        pass
    else:
        raise AssertionError("The internal queue should be empty")


@mock_sqs
def test_worker_fills_internal_queue_from_celery_task():
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    message = Message()
    body = '{"body": "KGRwMApTJ3Rhc2snCnAxClMndGVzdHMudGFza3MuaW5kZXhfaW5jcmVtZW50ZXInCnAyCnNTJ2Fy\\nZ3MnCnAzCihscDQKc1Mna3dhcmdzJwpwNQooZHA2ClMnbWVzc2FnZScKcDcKUydUZXN0IG1lc3Nh\\nZ2UyJwpwOApzcy4=\\n", "some stuff": "asdfasf"}'
    message.set_body(body)
    queue.write(message)

    internal_queue = Queue()
    worker = ReadWorker(queue, internal_queue)
    worker.read_message()

    found_message = internal_queue.get(timeout=1)

    found_message.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message2',
        },
    })


def test_worker_processes_tasks_from_internal_queue():
    message = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    }
    internal_queue = Queue()
    internal_queue.put(message)

    worker = ProcessWorker(internal_queue)
    worker.process_message()

    task_results.should.equal(['Test message'])

    try:
        internal_queue.get(timeout=1)
    except Empty:
        pass
    else:
        raise AssertionError("The internal queue should be empty")


class MockLoggingHandler(logging.Handler):
    """Mock logging handler to check for expected logs."""

    def __init__(self, *args, **kwargs):
        self.reset()
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.messages[record.levelname.lower()].append(record.getMessage())

    def reset(self):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': [],
        }


def test_worker_processes_tasks_and_logs_correctly():
    logger = logging.getLogger("pyqs")
    logger.handlers.append(MockLoggingHandler())
    message = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    }
    internal_queue = Queue()
    internal_queue.put(message)

    worker = ProcessWorker(internal_queue)
    worker.process_message()

    expected_result = "Processing task tests.tasks.index_incrementer with args: [] and kwargs: {'message': 'Test message'}"
    logger.handlers[0].messages['info'].should.equal([expected_result])


def test_worker_processes_tasks_and_logs_warning_correctly():
    logger = logging.getLogger("pyqs")
    logger.handlers.append(MockLoggingHandler())
    message = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }
    internal_queue = Queue()
    internal_queue.put(message)

    worker = ProcessWorker(internal_queue)
    worker.process_message()

    base_dir = abspath(join(dirname(__file__), pardir, pardir))
    expected_result = (
        "Task tests.tasks.index_incrementer raised error: with"
        " args: [] and kwargs: {'message': 23}: Traceback (most recent call last)"
        ':\n  File "%s/PyQS/pyqs/worker.py", line 101, in '
        "process_message\n    task(*args, **kwargs)\n  File "
        '"%s/PyQS/tests/tasks.py", line 11, in '
        'index_incrementer\n    raise ValueError("Need to be given basestring, was '
        'given {}".format(message))\nValueError: Need to be given basestring, was '
        "given 23\n" % (base_dir, base_dir)
    )
    logger.handlers[0].messages['error'].should.equal([expected_result])


def test_worker_processes_empty_queue():
    internal_queue = Queue()

    worker = ProcessWorker(internal_queue)
    worker.process_message()
