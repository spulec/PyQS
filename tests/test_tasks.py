import json

import boto3
from moto import mock_sqs, mock_sqs_deprecated

from .tasks import (
    index_incrementer, send_email, delayed_task, custom_path_task,
)


@mock_sqs()
@mock_sqs_deprecated()
def test_basic_delay():
    """
    Test delaying task to default queue
    """
    conn = boto3.client('sqs', region_name='us-east-1')
    conn.create_queue(QueueName="tests.tasks.index_incrementer")

    index_incrementer.delay("foobar", **{'extra': 'more'})

    all_queues = conn.list_queues().get('QueueUrls', [])
    len(all_queues).should.equal(1)

    queue_url = all_queues[0]
    queue_url.should.equal(
        "https://queue.amazonaws.com/123456789012/"
        "tests.tasks.index_incrementer"
    )
    queue = conn.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=['All'])['Attributes']
    queue['ApproximateNumberOfMessages'].should.equal('1')

    message = conn.receive_message(QueueUrl=queue_url)['Messages'][0]
    message_dict = json.loads(message['Body'])
    message_dict.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': ["foobar"],
        'kwargs': {'extra': 'more'},
    })


@mock_sqs()
@mock_sqs_deprecated()
def test_specified_queue():
    """
    Test delaying task to specific queue
    """
    conn = boto3.client('sqs', region_name='us-east-1')

    send_email.delay("email subject")

    queue_urls = conn.list_queues().get('QueueUrls', [])
    len(queue_urls).should.equal(1)

    queue_url = queue_urls[0]
    queue_url.should.equal("https://queue.amazonaws.com/123456789012/email")
    queue = conn.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=['All'])['Attributes']
    queue['ApproximateNumberOfMessages'].should.equal('1')


@mock_sqs()
@mock_sqs_deprecated()
def test_message_delay():
    """
    Test delaying task with delay_seconds
    """
    conn = boto3.client('sqs', region_name='us-east-1')

    delayed_task.delay()

    queue_urls = conn.list_queues().get('QueueUrls', [])
    len(queue_urls).should.equal(1)

    queue_url = queue_urls[0]
    queue_url.should.equal("https://queue.amazonaws.com/123456789012/delayed")
    queue = conn.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=['All'])['Attributes']
    queue['ApproximateNumberOfMessages'].should.equal('0')


@mock_sqs()
@mock_sqs_deprecated()
def test_message_add_delay():
    """
    Test configuring the delay time of a task
    """
    conn = boto3.client('sqs', region_name='us-east-1')

    send_email.delay("email subject", _delay_seconds=5)

    queue_urls = conn.list_queues().get('QueueUrls', [])
    len(queue_urls).should.equal(1)

    queue_url = queue_urls[0]
    queue_url.should.equal("https://queue.amazonaws.com/123456789012/email")
    queue = conn.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=['All'])['Attributes']
    queue['ApproximateNumberOfMessages'].should.equal('0')


@mock_sqs()
@mock_sqs_deprecated()
def test_message_no_delay():
    """
    Test removing the delay time of a task
    """
    conn = boto3.client('sqs', region_name='us-east-1')

    delayed_task.delay(_delay_seconds=0)

    queue_urls = conn.list_queues().get('QueueUrls', [])
    len(queue_urls).should.equal(1)

    queue_url = queue_urls[0]
    queue_url.should.equal("https://queue.amazonaws.com/123456789012/delayed")
    queue = conn.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=['All'])['Attributes']
    queue['ApproximateNumberOfMessages'].should.equal('1')


@mock_sqs()
@mock_sqs_deprecated()
def test_custom_function_path():
    """
    Test delaying task with custom function path
    """
    conn = boto3.client('sqs', region_name='us-east-1')

    custom_path_task.delay()

    queue_urls = conn.list_queues().get('QueueUrls', [])
    len(queue_urls).should.equal(1)
    queue_url = queue_urls[0]
    queue_url.should.equal("https://queue.amazonaws.com/123456789012/foobar")
    queue = conn.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=['All'])['Attributes']
    queue['ApproximateNumberOfMessages'].should.equal('1')

    message = conn.receive_message(QueueUrl=queue_url)['Messages'][0]
    message_dict = json.loads(message['Body'])
    message_dict.should.equal({
        'task': 'custom_function.path',
        'args': [],
        'kwargs': {},
    })
