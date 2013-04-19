import json

import boto
from boto.sqs.message import Message
from moto import mock_sqs
import sure  # flake8: noqa

from pyqs.worker import ReadWorker, ProcessWorker, internal_queue
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

    worker = ReadWorker(queue)
    worker.read_queue()

    found_message = internal_queue.get()

    found_message.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })


@mock_sqs
def test_worker_fills_internal_queue_from_celery_task():
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    message = Message()
    body = 'eyJib2R5IjogIktHUndNQXBUSjNSaGMyc25DbkF4Q2xNbmRHVnpkSE11ZEdGemEzTXVhVzVrWlho\nZmFXNWpjbVZ0Wlc1MFpYSW5DbkF5Q25OVEoyRnlcblozTW5DbkF6Q2loc2NEUUtjMU1uYTNkaGNt\nZHpKd3B3TlFvb1pIQTJDbE1uYldWemMyRm5aU2NLY0RjS1V5ZFVaWE4wSUcxbGMzTmhcbloyVXlK\nd3B3T0FwemN5ND1cbiIsICJzb21lIHN0dWZmIjogImFzZGZhc2YifQ==\n'
    message.set_body(body)
    queue.write(message)

    worker = ReadWorker(queue)
    worker.read_queue()

    found_message = internal_queue.get()

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
    worker = ProcessWorker()
    internal_queue.put(message)

    worker.process_messages()

    task_results.should.equal(['Test message'])
