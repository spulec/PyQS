import json

import boto
from boto.sqs.message import Message
from moto import mock_sqs
import sure  # flake8: noqa

from pyqs.worker import main
from tests.tasks import task_results


@mock_sqs
def test_basic_worker_pull():
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

    main("tester")

    task_results.should.equal(['Test message'])
