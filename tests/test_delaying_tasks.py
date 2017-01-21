import json

import boto
from moto import mock_sqs

from .tasks import index_incrementer, send_email, delayed_task


@mock_sqs()
def test_basic_delay():
    """
    Test delaying task to default queue
    """
    conn = boto.connect_sqs()
    conn.create_queue("tests.tasks.index_incrementer")

    index_incrementer.delay("foobar", **{'extra': 'more'})

    all_queues = conn.get_all_queues()
    len(all_queues).should.equal(1)

    queue = all_queues[0]
    queue.name.should.equal("tests.tasks.index_incrementer")
    queue.count().should.equal(1)

    message = queue.get_messages()[0].get_body()
    message_dict = json.loads(message)
    message_dict.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': ["foobar"],
        'kwargs': {'extra': 'more'},
    })


@mock_sqs()
def test_specified_queue():
    """
    Test delaying task to specific queue
    """
    conn = boto.connect_sqs()

    send_email.delay("email subject")

    all_queues = conn.get_all_queues()
    len(all_queues).should.equal(1)

    queue = all_queues[0]
    queue.name.should.equal("email")
    queue.count().should.equal(1)


@mock_sqs()
def test_message_delay():
    """
    Test delaying task to specific queue
    """

    conn = boto.connect_sqs()

    delayed_task.delay()

    all_queues = conn.get_all_queues()
    len(all_queues).should.equal(1)

    queue = all_queues[0]
    queue.name.should.equal("delayed")
    queue.count().should.equal(0)
