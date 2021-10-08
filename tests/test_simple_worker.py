import json
import logging
import time

import boto3
from botocore.exceptions import ClientError
from moto import mock_sqs
from mock import patch, Mock
from pyqs.worker import (
    SimpleManagerWorker, BaseProcessWorker, SimpleProcessWorker,
    MESSAGE_DOWNLOAD_BATCH_SIZE
)
from pyqs.utils import decode_message
from pyqs.events import register_event
from tests.utils import MockLoggingHandler, clear_events_registry

BATCHSIZE = 10
INTERVAL = 0.1


def _create_packed_message(task_name):
    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Build the SQS message
    message = {
        'Body': json.dumps({
            'task': task_name,
            'args': [],
            'kwargs': {
                'message': 'Test message',
            },
        }),
        "ReceiptHandle": "receipt-1234",
        "MessageId": "message-id-1",
    }

    packed_message = {
        "queue": queue_url,
        "message": message,
        "start_time": time.time(),
        "timeout": 30,
    }

    return queue_url, packed_message


def _add_messages_to_sqs(task_name, num):
    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Build the SQS message
    message = json.dumps({
        'task': task_name,
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })

    for i in range(num):
        conn.send_message(QueueUrl=queue_url, MessageBody=message)

    return queue_url


@mock_sqs
def test_worker_reads_messages_from_sqs():
    """
    Test simple worker reads from sqs queue
    """
    queue_url = _add_messages_to_sqs('tests.tasks.index_incrementer', 1)

    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    messages = worker.read_message()

    found_message_body = decode_message(messages[0])
    found_message_body.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })


@mock_sqs
def test_worker_throws_error_when_exceeding_max_number_of_messages_for_read():
    """
    Test simple worker reads from sqs queue and throws error when batchsize
    greater than 10
    """
    queue_url = _add_messages_to_sqs('tests.tasks.index_incrementer', 1)

    worker = SimpleProcessWorker(queue_url, INTERVAL, 20, parent_id=1)

    error_msg = "Value 20 for parameter MaxNumberOfMessages is invalid"

    try:
        worker.read_message()
    except ClientError as exc:
        str(exc).should.contain(error_msg)


@mock_sqs
def test_worker_reads_max_messages_from_sqs():
    """
    Test simple worker reads at maximum 10 message from sqs queue
    """
    _add_messages_to_sqs('tests.tasks.index_incrementer', 12)

    manager = SimpleManagerWorker(
        queue_prefixes=['tester'], worker_concurrency=1, interval=INTERVAL,
        batchsize=20,
    )
    worker = manager.worker_children[0]
    messages = worker.read_message()

    messages.should.have.length_of(BATCHSIZE)


@mock_sqs
def test_worker_fills_internal_queue_from_celery_task():
    """
    Test simple worker reads from sqs queue with celery tasks
    """
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    message = (
        '{"body": "KGRwMApTJ3Rhc2snCnAxClMndGVzdHMudGFza3MuaW5kZXhfa'
        'W5jcmVtZW50ZXInCnAyCnNTJ2Fy\\nZ3MnCnAzCihscDQKc1Mna3dhcmdzJw'
        'pwNQooZHA2ClMnbWVzc2FnZScKcDcKUydUZXN0IG1lc3Nh\\nZ2UyJwpwOAp'
        'zcy4=\\n", "some stuff": "asdfasf"}'
    )
    conn.send_message(QueueUrl=queue_url, MessageBody=message)

    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    messages = worker.read_message()

    found_message_body = decode_message(messages[0])
    found_message_body.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message2',
        },
    })


@mock_sqs
def test_worker_processes_tasks_and_logs_correctly():
    """
    Test simple worker processes logs INFO correctly
    """
    # Setup logging
    logger = logging.getLogger("pyqs")
    del logger.handlers[:]
    logger.handlers.append(MockLoggingHandler())

    queue_url, packed_message = _create_packed_message(
        'tests.tasks.index_incrementer'
    )

    # Process message
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message(packed_message)

    # Check output
    kwargs = json.loads(packed_message["message"]['Body'])['kwargs']
    expected_result = (
        u"Processed task tests.tasks.index_incrementer in 0.0000 seconds "
        "with args: [] and kwargs: {}".format(kwargs)
    )
    logger.handlers[0].messages['info'].should.equal([expected_result])


@mock_sqs
def test_worker_processes_tasks_and_logs_warning_correctly():
    """
    Test simple worker processes logs WARNING correctly
    """
    # Setup logging
    logger = logging.getLogger("pyqs")
    del logger.handlers[:]
    logger.handlers.append(MockLoggingHandler())

    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Build the SQS Message
    message = {
        'Body': json.dumps({
            'task': 'tests.tasks.index_incrementer',
            'args': [],
            'kwargs': {
                'message': 23,
            },
        }),
        "ReceiptHandle": "receipt-1234",
        "MessageId": "message-id-1",
    }

    packed_message = {
        "queue": queue_url,
        "message": message,
        "start_time": time.time(),
        "timeout": 30,
    }

    # Process message
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message(packed_message)

    # Check output
    kwargs = json.loads(message['Body'])['kwargs']
    msg1 = (
        "Task tests.tasks.index_incrementer raised error in 0.0000 seconds: "
        "with args: [] and kwargs: {}: "
        "Traceback (most recent call last)".format(kwargs)
    )  # noqa
    logger.handlers[0].messages['error'][0].lower().should.contain(
        msg1.lower())
    msg2 = (
        'ValueError: Need to be given basestring, '
        'was given 23'
    )  # noqa
    logger.handlers[0].messages['error'][0].lower().should.contain(
        msg2.lower())


@patch("pyqs.worker.os")
def test_parent_process_death(os):
    """
    Test simple worker processes recognize parent process death
    """
    os.getppid.return_value = 123

    worker = BaseProcessWorker(parent_id=1)
    worker.parent_is_alive().should.be.false


@patch("pyqs.worker.os")
def test_parent_process_alive(os):
    """
    Test simple worker processes recognize when parent process is alive
    """
    os.getppid.return_value = 1234

    worker = BaseProcessWorker(parent_id=1234)
    worker.parent_is_alive().should.be.true


@mock_sqs
@patch("pyqs.worker.os")
def test_read_message_with_parent_process_alive_and_should_not_exit(os):
    """
    Test simple worker processes do not exit when parent is alive and shutdown
    is not set when reading message
    """
    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Setup PPID
    os.getppid.return_value = 1

    # Setup dummy read_message
    def read_message():
        raise Exception("Called")

    # When I have a parent process, and shutdown is not set
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.read_message = read_message

    # Then read_message() is reached
    worker.run.when.called_with().should.throw(Exception, "Called")


@mock_sqs
@patch("pyqs.worker.os")
def test_read_message_with_parent_process_alive_and_should_exit(os):
    """
    Test simple worker processes exit when parent is alive and shutdown is set
    when reading message
    """
    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Setup PPID
    os.getppid.return_value = 1234

    # When I have a parent process, and shutdown is set
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.read_message = Mock()
    worker.shutdown()

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_read_message_with_parent_process_dead_and_should_not_exit(os):
    """
    Test simple worker processes exit when parent is dead and shutdown is not
    set when reading messages
    """
    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Setup PPID
    os.getppid.return_value = 123

    # When I have no parent process, and shutdown is not set
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.read_message = Mock()

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_process_message_with_parent_process_alive_and_should_not_exit(os):
    """
    Test simple worker processes do not exit when parent is alive and shutdown
    is not set when processing message
    """
    queue_url = _add_messages_to_sqs('tests.tasks.index_incrementer', 1)

    # Setup PPID
    os.getppid.return_value = 1

    # Setup dummy read_message
    def process_message(packed_message):
        raise Exception("Called")

    # When I have a parent process, and shutdown is not set
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message = process_message

    # Then process_message() is reached
    worker.run.when.called_with().should.throw(Exception, "Called")


@mock_sqs
@patch("pyqs.worker.os")
def test_process_message_with_parent_process_dead_and_should_not_exit(os):
    """
    Test simple worker processes exit when parent is dead and shutdown is not
    set when processing message
    """

    queue_url = _add_messages_to_sqs('tests.tasks.index_incrementer', 1)

    # Setup PPID
    os.getppid.return_value = 123

    # When I have a parent process, and shutdown is not set
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message = Mock()

    # Then process_message() is reached
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_process_message_with_parent_process_alive_and_should_exit(os):
    """
    Test simple worker processes exit when parent is alive and shutdown is set
    set when processing message
    """

    queue_url = _add_messages_to_sqs('tests.tasks.index_incrementer', 1)

    # Setup PPID
    os.getppid.return_value = 1

    # When I have a parent process, and shutdown is not set
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message = Mock()
    worker.shutdown()

    # Then process_message() is reached
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_worker_processes_shuts_down_after_processing_its_max_number_of_msgs(
        os):
    """
    Test simple worker processes shutdown after processing maximum number
    of messages
    """
    os.getppid.return_value = 1

    queue_url = _add_messages_to_sqs('tests.tasks.index_incrementer', 2)

    # When I Process messages
    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker._messages_to_process_before_shutdown = 2

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
def test_worker_negative_batch_size():
    """
    Test simple workers with negative batch sizes
    """
    BATCHSIZE = -1
    CONCURRENCY = 1
    QUEUE_PREFIX = "tester"
    INTERVAL = 0.0
    conn = boto3.client('sqs', region_name='us-east-1')
    conn.create_queue(QueueName="tester")['QueueUrl']

    worker = SimpleManagerWorker(
        QUEUE_PREFIX,
        CONCURRENCY,
        INTERVAL,
        BATCHSIZE
    )
    worker.batchsize.should.equal(1)


@mock_sqs
def test_worker_to_large_batch_size():
    """
    Test simple workers with too large of a batch size
    """
    BATCHSIZE = 10000
    CONCURRENCY = 1
    QUEUE_PREFIX = "tester"
    INTERVAL = 0.0
    conn = boto3.client('sqs', region_name='us-east-1')
    conn.create_queue(QueueName="tester")['QueueUrl']

    worker = SimpleManagerWorker(
        QUEUE_PREFIX,
        CONCURRENCY,
        INTERVAL,
        BATCHSIZE
    )
    worker.batchsize.should.equal(MESSAGE_DOWNLOAD_BATCH_SIZE)


@clear_events_registry
@mock_sqs
def test_worker_processes_tasks_with_pre_process_callback():
    """
    Test simple worker runs registered callbacks when processing a message
    """

    queue_url, packed_message = _create_packed_message(
        'tests.tasks.index_incrementer'
    )

    # Declare this so it can be checked as a side effect
    # to pre_process_with_side_effect
    contexts = []

    def pre_process_with_side_effect(context):
        contexts.append(context)

    # When we have a registered pre_process callback
    register_event("pre_process", pre_process_with_side_effect)

    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message(packed_message)

    pre_process_context = contexts[0]

    # We should run the callback with the task context
    pre_process_context['task_name'].should.equal('index_incrementer')
    pre_process_context['args'].should.equal([])
    pre_process_context['kwargs'].should.equal({'message': 'Test message'})
    pre_process_context['full_task_path'].should.equal(
        'tests.tasks.index_incrementer'
    )
    pre_process_context['queue_url'].should.equal(
        'https://queue.amazonaws.com/123456789012/tester'
    )
    pre_process_context['timeout'].should.equal(30)

    assert 'fetch_time' in pre_process_context
    assert 'receipt_handle' in pre_process_context
    assert 'status' not in pre_process_context


@clear_events_registry
@mock_sqs
def test_worker_processes_tasks_with_post_process_callback_success():
    """
    Test simple worker runs registered callbacks when
    processing a message and it succeeds
    """

    queue_url, packed_message = _create_packed_message(
        'tests.tasks.index_incrementer'
    )

    # Declare this so it can be checked as a side effect
    # to post_process_with_side_effect
    contexts = []

    def post_process_with_side_effect(context):
        contexts.append(context)

    # When we have a registered post_process callback
    register_event("post_process", post_process_with_side_effect)

    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message(packed_message)

    post_process_context = contexts[0]

    # We should run the callback with the task context
    post_process_context['task_name'].should.equal('index_incrementer')
    post_process_context['args'].should.equal([])
    post_process_context['kwargs'].should.equal({'message': 'Test message'})
    post_process_context['full_task_path'].should.equal(
        'tests.tasks.index_incrementer'
    )
    post_process_context['queue_url'].should.equal(
        'https://queue.amazonaws.com/123456789012/tester'
    )
    post_process_context['timeout'].should.equal(30)
    post_process_context['status'].should.equal('success')

    assert 'fetch_time' in post_process_context
    assert 'receipt_handle' in post_process_context
    assert 'exception' not in post_process_context


@clear_events_registry
@mock_sqs
def test_worker_processes_tasks_with_post_process_callback_exception():
    """
    Test simple worker runs registered callbacks when processing
    a message and it fails
    """

    queue_url, packed_message = _create_packed_message(
        'tests.tasks.exception_task'
    )

    # Declare this so it can be checked as a side effect
    # to post_process_with_side_effect
    contexts = []

    def post_process_with_side_effect(context):
        contexts.append(context)

    # When we have a registered post_process callback
    register_event("post_process", post_process_with_side_effect)

    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message(packed_message)

    post_process_context = contexts[0]

    # We should run the callback with the task context
    post_process_context['task_name'].should.equal('exception_task')
    post_process_context['args'].should.equal([])
    post_process_context['kwargs'].should.equal({'message': 'Test message'})
    post_process_context['full_task_path'].should.equal(
        'tests.tasks.exception_task'
    )
    post_process_context['queue_url'].should.equal(
        'https://queue.amazonaws.com/123456789012/tester'
    )
    post_process_context['timeout'].should.equal(30)
    post_process_context['status'].should.equal('exception')

    assert 'fetch_time' in post_process_context
    assert 'receipt_handle' in post_process_context
    assert 'exception' in post_process_context


@clear_events_registry
@mock_sqs
def test_worker_processes_tasks_with_pre_and_post_process():
    """
    Test worker runs registered callbacks when processing a message
    """

    queue_url, packed_message = _create_packed_message(
        'tests.tasks.index_incrementer'
    )

    # Declare these so they can be checked as a side effect to the callbacks
    contexts = []

    def pre_process_with_side_effect(context):
        contexts.append(context)

    def post_process_with_side_effect(context):
        contexts.append(context)

    # When we have a registered pre_process and post_process callback
    register_event("pre_process", pre_process_with_side_effect)
    register_event("post_process", post_process_with_side_effect)

    worker = SimpleProcessWorker(queue_url, INTERVAL, BATCHSIZE, parent_id=1)
    worker.process_message(packed_message)

    pre_process_context = contexts[0]

    # We should run the callbacks with the right task contexts
    pre_process_context['task_name'].should.equal('index_incrementer')
    pre_process_context['args'].should.equal([])
    pre_process_context['kwargs'].should.equal({'message': 'Test message'})
    pre_process_context['full_task_path'].should.equal(
        'tests.tasks.index_incrementer'
    )
    pre_process_context['queue_url'].should.equal(
        'https://queue.amazonaws.com/123456789012/tester'
    )
    pre_process_context['timeout'].should.equal(30)

    assert 'fetch_time' in pre_process_context
    assert 'receipt_handle' in pre_process_context
    assert 'status' not in pre_process_context

    post_process_context = contexts[1]

    post_process_context['task_name'].should.equal('index_incrementer')
    post_process_context['args'].should.equal([])
    post_process_context['kwargs'].should.equal({'message': 'Test message'})
    post_process_context['full_task_path'].should.equal(
        'tests.tasks.index_incrementer'
    )
    post_process_context['queue_url'].should.equal(
        'https://queue.amazonaws.com/123456789012/tester'
    )
    post_process_context['timeout'].should.equal(30)
    post_process_context['status'].should.equal('success')

    assert 'fetch_time' in post_process_context
    assert 'receipt_handle' in post_process_context
    assert 'exception' not in post_process_context
