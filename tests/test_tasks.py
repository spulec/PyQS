import json

import boto3
from moto import mock_sqs

from .tasks import index_incrementer, send_email, delayed_task, custom_path_task


@mock_sqs()
def test_basic_delay():
    """
    Test delaying task to default queue
    """
    client = boto3.client('sqs')
    client.create_queue(QueueName="tests-tasks-index_incrementer")

    index_incrementer.delay("foobar", **{'extra': 'more'})

    all_queues = boto3.client('sqs').list_queues()['QueueUrls']
    len(all_queues).should.equal(1)

    queue = boto3.resource('sqs').Queue(all_queues[0])
    queue.url.should.contain("tests-tasks-index_incrementer")
    int(queue.attributes['ApproximateNumberOfMessages']).should.equal(1)

    message = queue.receive_messages()[0].body
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
    send_email.delay("email subject")

    all_queues = boto3.client('sqs').list_queues()['QueueUrls']
    len(all_queues).should.equal(1)

    queue = boto3.resource('sqs').Queue(all_queues[0])
    queue.url.should.contain("email")

    int(queue.attributes['ApproximateNumberOfMessages']).should.equal(1)


@mock_sqs()
def test_message_delay():
    """
    Test delaying task with delay_seconds
    """
    delayed_task.delay()

    all_queues = boto3.client('sqs').list_queues()['QueueUrls']
    len(all_queues).should.equal(1)

    queue = boto3.resource('sqs').Queue(all_queues[0])
    queue.url.should.contain("delayed")

    int(queue.attributes['ApproximateNumberOfMessages']).should.equal(0)


@mock_sqs()
def test_message_add_delay():
    """
    Test configuring the delay time of a task
    """
    send_email.delay("email subject", _delay_seconds=5)

    all_queues = boto3.client('sqs').list_queues()['QueueUrls']
    len(all_queues).should.equal(1)

    queue = boto3.resource('sqs').Queue(all_queues[0])
    queue.url.should.contain("email")

    int(queue.attributes['ApproximateNumberOfMessages']).should.equal(0)


@mock_sqs()
def test_message_no_delay():
    """
    Test removing the delay time of a task
    """
    delayed_task.delay(_delay_seconds=0)

    all_queues = boto3.client('sqs').list_queues()['QueueUrls']
    len(all_queues).should.equal(1)

    queue = boto3.resource('sqs').Queue(all_queues[0])
    queue.url.should.contain("delayed")
    int(queue.attributes['ApproximateNumberOfMessages']).should.equal(1)


@mock_sqs()
def test_custom_function_path():
    """
    Test delaying task with custom function path
    """
    custom_path_task.delay()

    all_queues = boto3.client('sqs').list_queues()['QueueUrls']
    queue = boto3.resource('sqs').Queue(all_queues[0])
    queue.url.should.contain("foobar")
    int(queue.attributes['ApproximateNumberOfMessages']).should.equal(1)

    message = queue.receive_messages()[0].body
    message_dict = json.loads(message)
    message_dict.should.equal({
        'task': 'custom_function.path',
        'args': [],
        'kwargs': {},
    })
