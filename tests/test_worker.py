import json
import logging
import time
import threading

from multiprocessing import Queue
try:
    from queue import Empty
except ImportError:
    from Queue import Empty

import boto3
from moto import mock_sqs
from mock import patch, Mock
from pyqs.worker import (
    ManagerWorker, ReadWorker, ProcessWorker, BaseWorker,
    MESSAGE_DOWNLOAD_BATCH_SIZE,
)
from pyqs.utils import decode_message
from pyqs.events import register_event
from tests.tasks import task_results
from tests.utils import MockLoggingHandler, clear_events_registry

BATCHSIZE = 10
INTERVAL = 0.1


def _add_message_to_internal_queue(task_name):
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
    # Add message to queue
    internal_queue = Queue()
    internal_queue.put(
        {
            "message": message,
            "queue": queue_url,
            "start_time": time.time(),
            "timeout": 30,
        }
    )
    return internal_queue


def _check_internal_queue_is_empty(internal_queue):
    try:
        internal_queue.get(timeout=1)
    except Empty:
        pass
    else:
        raise AssertionError("The internal queue should be empty")


@mock_sqs
def test_worker_fills_internal_queue():
    """
    Test read workers fill internal queue
    """
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    message = json.dumps({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })

    conn.send_message(QueueUrl=queue_url, MessageBody=message)

    internal_queue = Queue()
    worker = ReadWorker(queue_url, internal_queue, BATCHSIZE, parent_id=1)
    worker.read_message()

    packed_message = internal_queue.get(timeout=1)
    found_message_body = decode_message(packed_message['message'])
    found_message_body.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })


@mock_sqs
def test_worker_fills_internal_queue_only_until_maximum_queue_size():
    """
    Test read workers fill internal queue only to maximum size
    """
    conn = boto3.client('sqs', region_name='us-east-1')
    # Set visibility timeout low to improve test speed
    queue_url = conn.create_queue(
        QueueName="tester", Attributes={'VisibilityTimeout': '1'})['QueueUrl']

    message = json.dumps({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })
    for i in range(3):
        conn.send_message(QueueUrl=queue_url, MessageBody=message)

    internal_queue = Queue(maxsize=2)
    worker = ReadWorker(queue_url, internal_queue, BATCHSIZE, parent_id=1)
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
    """
    Test read workers fill internal queue with celery tasks
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

    internal_queue = Queue()
    worker = ReadWorker(queue_url, internal_queue, BATCHSIZE, parent_id=1)
    worker.read_message()

    packed_message = internal_queue.get(timeout=1)
    found_message_body = decode_message(packed_message['message'])
    found_message_body.should.equal({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message2',
        },
    })


@mock_sqs
def test_worker_processes_tasks_from_internal_queue():
    """
    Test worker processes read from internal queue
    """
    del task_results[:]

    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Build the SQS message
    message = {
        'Body': json.dumps({
            'task': 'tests.tasks.index_incrementer',
            'args': [],
            'kwargs': {
                'message': 'Test message',
            },
        }),
        "ReceiptHandle": "receipt-1234",
        "MessageId": "message-id-1",
    }

    # Add message to queue
    internal_queue = Queue()
    internal_queue.put(
        {
            "message": message,
            "queue": queue_url,
            "start_time": time.time(),
            "timeout": 30,
        }
    )

    # Process message
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()

    task_results.should.equal(['Test message'])

    # We expect the queue to be empty now
    try:
        internal_queue.get(timeout=1)
    except Empty:
        pass
    else:
        raise AssertionError("The internal queue should be empty")


@mock_sqs
def test_worker_fills_internal_queue_and_respects_visibility_timeouts():
    """
    Test read workers respect visibility timeouts
    """
    # Setup logging
    logger = logging.getLogger("pyqs")
    logger.handlers.append(MockLoggingHandler())

    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(
        QueueName="tester", Attributes={'VisibilityTimeout': '1'})['QueueUrl']

    # Add MEssages
    message = json.dumps(
        {
            "body": (
                "KGRwMApTJ3Rhc2snCnAxClMndGVzdHMudGFza3MuaW5kZXhfaW5jcmVtZW"
                "50ZXInCnAyCnNTJ2Fy\nZ3MnCnAzCihscDQKc1Mna3dhcmdzJwpwNQooZHA"
                "2ClMnbWVzc2FnZScKcDcKUydUZXN0IG1lc3Nh\nZ2UyJwpwOApzcy4=\n"
            ),
            "some stuff": "asdfasf",
        }
    )
    for _ in range(3):
        conn.send_message(QueueUrl=queue_url, MessageBody=message)

    # Run Reader
    internal_queue = Queue(maxsize=1)
    worker = ReadWorker(queue_url, internal_queue, BATCHSIZE, parent_id=1)
    worker.read_message()

    # Check log messages
    logger.handlers[0].messages['warning'][0].should.contain(
        "Timed out trying to add the following message to the internal queue")
    logger.handlers[0].messages['warning'][1].should.contain(
        "Clearing Local messages since we exceeded their visibility_timeout")


@mock_sqs
def test_worker_processes_tasks_and_logs_correctly():
    """
    Test worker processes logs INFO correctly
    """
    # Setup logging
    logger = logging.getLogger("pyqs")
    del logger.handlers[:]
    logger.handlers.append(MockLoggingHandler())

    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Build the SQS message
    message = {
        'Body': json.dumps({
            'task': 'tests.tasks.index_incrementer',
            'args': [],
            'kwargs': {
                'message': 'Test message',
            },
        }),
        "ReceiptHandle": "receipt-1234",
        "MessageId": "message-id-1",
    }

    # Add message to internal queue
    internal_queue = Queue()
    internal_queue.put(
        {
            "queue": queue_url,
            "message": message,
            "start_time": time.time(),
            "timeout": 30,
        }
    )

    # Process message
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()

    # Check output
    kwargs = json.loads(message['Body'])['kwargs']
    expected_result = (
        u"Processed task tests.tasks.index_incrementer in 0.0000 seconds "
        "with args: [] and kwargs: {}".format(kwargs)
    )
    logger.handlers[0].messages['info'].should.equal([expected_result])


@mock_sqs
def test_worker_processes_tasks_and_logs_warning_correctly():
    """
    Test worker processes logs WARNING correctly
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

    # Add message to internal queue
    internal_queue = Queue()
    internal_queue.put(
        {
            "queue": queue_url,
            "message": message,
            "start_time": time.time(),
            "timeout": 30,
        }
    )

    # Process message
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()

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


@mock_sqs
def test_worker_processes_empty_queue():
    """
    Test worker processes read from empty internal queue
    """
    internal_queue = Queue()

    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()


@patch("pyqs.worker.os")
def test_parent_process_death(os):
    """
    Test worker processes recognize parent process death
    """
    os.getppid.return_value = 123

    worker = BaseWorker(parent_id=1)
    worker.parent_is_alive().should.be.false


@patch("pyqs.worker.os")
def test_parent_process_alive(os):
    """
    Test worker processes recognize when parent process is alive
    """
    os.getppid.return_value = 1234

    worker = BaseWorker(parent_id=1234)
    worker.parent_is_alive().should.be.true


@mock_sqs
@patch("pyqs.worker.os")
def test_read_worker_with_parent_process_alive_and_should_not_exit(os):
    """
    Test read workers do not exit when parent is alive and shutdown is not set
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
    worker = ReadWorker(queue_url, "foo", BATCHSIZE, parent_id=1)
    worker.read_message = read_message

    # Then read_message() is reached
    worker.run.when.called_with().should.throw(Exception, "Called")


@mock_sqs
@patch("pyqs.worker.os")
def test_read_worker_with_parent_process_alive_and_should_exit(os):
    """
    Test read workers exit when parent is alive and shutdown is set
    """
    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Setup PPID
    os.getppid.return_value = 1234

    # Setup internal queue
    q = Queue(1)

    # When I have a parent process, and shutdown is set
    worker = ReadWorker(queue_url, q, BATCHSIZE, parent_id=1)
    worker.read_message = Mock()
    worker.shutdown()

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_read_worker_with_parent_process_dead_and_should_not_exit(os):
    """
    Test read workers exit when parent is dead and shutdown is not set
    """
    # Setup SQS Queue
    conn = boto3.client('sqs', region_name='us-east-1')
    queue_url = conn.create_queue(QueueName="tester")['QueueUrl']

    # Setup PPID
    os.getppid.return_value = 123

    # Setup internal queue
    q = Queue(1)

    # When I have no parent process, and shutdown is not set
    worker = ReadWorker(queue_url, q, BATCHSIZE, parent_id=1)
    worker.read_message = Mock()

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_process_worker_with_parent_process_alive_and_should_not_exit(os):
    """
    Test worker processes do not exit when parent is alive and shutdown
    is not set
    """
    # Setup PPID
    os.getppid.return_value = 1

    # Setup dummy read_message
    def process_message():
        raise Exception("Called")

    # When I have a parent process, and shutdown is not set
    worker = ProcessWorker("foo", INTERVAL, parent_id=1)
    worker.process_message = process_message

    # Then process_message() is reached
    worker.run.when.called_with().should.throw(Exception, "Called")


@mock_sqs
@patch("pyqs.worker.os")
def test_process_worker_with_parent_process_dead_and_should_not_exit(os):
    """
    Test worker processes exit when parent is dead and shutdown is not set
    """
    # Setup PPID
    os.getppid.return_value = 1

    # When I have no parent process, and shutdown is not set
    worker = ProcessWorker("foo", INTERVAL, parent_id=1)
    worker.process_message = Mock()

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_process_worker_with_parent_process_alive_and_should_exit(os):
    """
    Test worker processes exit when parent is alive and shutdown is set
    """
    # Setup PPID
    os.getppid.return_value = 1234

    # When I have a parent process, and shutdown is set
    worker = ProcessWorker("foo", INTERVAL, parent_id=1)
    worker.process_message = Mock()
    worker.shutdown()

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_worker_processes_shuts_down_after_processing_its_max_number_of_msgs(
        os):
    """
    Test worker processes shutdown after processing maximum number of messages
    """
    os.getppid.return_value = 1

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

    # Add message to internal queue
    internal_queue = Queue(3)
    internal_queue.put(
        {
            "queue": queue_url,
            "message": message,
            "start_time": time.time(),
            "timeout": 30,
        }
    )
    internal_queue.put(
        {
            "queue": queue_url,
            "message": message,
            "start_time": time.time(),
            "timeout": 30,
        }
    )
    internal_queue.put(
        {
            "queue": queue_url,
            "message": message,
            "start_time": time.time(),
            "timeout": 30,
        }
    )

    # When I Process messages
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker._messages_to_process_before_shutdown = 2

    # Then I return from run()
    worker.run().should.be.none

    # With messages still on the queue
    internal_queue.empty().should.be.false
    internal_queue.full().should.be.false


@mock_sqs
def test_worker_processes_discard_tasks_that_exceed_their_visibility_timeout():
    """
    Test worker processes discards tasks that exceed their visibility timeout
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

    # Add message to internal queue with timeout of 0 that started long ago
    internal_queue = Queue()
    internal_queue.put(
        {
            "queue": queue_url,
            "message": message,
            "start_time": 0,
            "timeout": 0,
        }
    )

    # When I process the message
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()

    # Then I get an error about exceeding the visibility timeout
    kwargs = json.loads(message['Body'])['kwargs']
    msg1 = (
        "Discarding task tests.tasks.index_incrementer with args: [] "
        "and kwargs: {} due to exceeding "
        "visibility timeout"
    ).format(kwargs)  # noqa
    logger.handlers[0].messages['warning'][0].lower().should.contain(
        msg1.lower())


@mock_sqs
def test_worker_processes_only_incr_processed_counter_if_a_msg_was_processed():
    """
    Test worker process only increases processed counter if a message was
    processed
    """
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

    # Add message to internal queue
    internal_queue = Queue(3)
    internal_queue.put(
        {
            "queue": queue_url,
            "message": message,
            "start_time": time.time(),
            "timeout": 30,
        }
    )

    # And we add a message to the queue later
    def sleep_and_queue(internal_queue):
        time.sleep(1)
        internal_queue.put(
            {
                "queue": queue_url,
                "message": message,
                "start_time": time.time(),
                "timeout": 30,
            }
        )

    thread = threading.Thread(target=sleep_and_queue, args=(internal_queue,))
    thread.daemon = True
    thread.start()

    # When I Process messages
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker._messages_to_process_before_shutdown = 2

    # Then I return from run() after processing 2 messages
    worker.run().should.be.none


@mock_sqs
def test_worker_negative_batch_size():
    """
    Test workers with negative batch sizes
    """
    BATCHSIZE = -1
    CONCURRENCY = 1
    QUEUE_PREFIX = "tester"
    INTERVAL = 0.0
    conn = boto3.client('sqs', region_name='us-east-1')
    conn.create_queue(QueueName="tester")['QueueUrl']

    worker = ManagerWorker(QUEUE_PREFIX, CONCURRENCY, INTERVAL, BATCHSIZE)
    worker.batchsize.should.equal(1)


@mock_sqs
def test_worker_to_large_batch_size():
    """
    Test workers with too large of a batch size
    """
    BATCHSIZE = 10000
    CONCURRENCY = 1
    QUEUE_PREFIX = "tester"
    INTERVAL = 0.0
    conn = boto3.client('sqs', region_name='us-east-1')
    conn.create_queue(QueueName="tester")['QueueUrl']

    worker = ManagerWorker(QUEUE_PREFIX, CONCURRENCY, INTERVAL, BATCHSIZE)
    worker.batchsize.should.equal(MESSAGE_DOWNLOAD_BATCH_SIZE)


@clear_events_registry
@mock_sqs
def test_worker_processes_tasks_with_pre_process_callback():
    """
    Test worker runs registered callbacks when processing a message
    """

    # Declare this so it can be checked as a side effect
    # to pre_process_with_side_effect
    contexts = []

    def pre_process_with_side_effect(context):
        contexts.append(context)

    # When we have a registered pre_process callback
    register_event("pre_process", pre_process_with_side_effect)

    # And we process a message
    internal_queue = _add_message_to_internal_queue(
        'tests.tasks.index_incrementer'
    )
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()

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
    assert 'status' not in pre_process_context

    # And the internal queue should be empty
    _check_internal_queue_is_empty(internal_queue)


@clear_events_registry
@mock_sqs
def test_worker_processes_tasks_with_post_process_callback_success():
    """
    Test worker runs registered callbacks when
    processing a message and it succeeds
    """

    # Declare this so it can be checked as a side effect
    # to post_process_with_side_effect
    contexts = []

    def post_process_with_side_effect(context):
        contexts.append(context)

    # When we have a registered post_process callback
    register_event("post_process", post_process_with_side_effect)

    # And we process a message
    internal_queue = _add_message_to_internal_queue(
        'tests.tasks.index_incrementer'
    )
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()

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
    assert 'exception' not in post_process_context

    # And the internal queue should be empty
    _check_internal_queue_is_empty(internal_queue)


@clear_events_registry
@mock_sqs
def test_worker_processes_tasks_with_post_process_callback_exception():
    """
    Test worker runs registered callbacks when processing
    a message and it fails
    """

    # Declare this so it can be checked as a side effect
    # to post_process_with_side_effect
    contexts = []

    def post_process_with_side_effect(context):
        contexts.append(context)

    # When we have a registered post_process callback
    register_event("post_process", post_process_with_side_effect)

    # And we process a message
    internal_queue = _add_message_to_internal_queue(
        'tests.tasks.exception_task'
    )
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()

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
    assert 'exception' in post_process_context

    # And the internal queue should be empty
    _check_internal_queue_is_empty(internal_queue)


@clear_events_registry
@mock_sqs
def test_worker_processes_tasks_with_pre_and_post_process():
    """
    Test worker runs registered callbacks when processing a message
    """

    # Declare these so they can be checked as a side effect to the callbacks
    contexts = []

    def pre_process_with_side_effect(context):
        contexts.append(context)

    def post_process_with_side_effect(context):
        contexts.append(context)

    # When we have a registered pre_process and post_process callback
    register_event("pre_process", pre_process_with_side_effect)
    register_event("post_process", post_process_with_side_effect)

    # And we process a message
    internal_queue = _add_message_to_internal_queue(
        'tests.tasks.index_incrementer'
    )
    worker = ProcessWorker(internal_queue, INTERVAL, parent_id=1)
    worker.process_message()

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
    assert 'exception' not in post_process_context

    # And the internal queue should be empty
    _check_internal_queue_is_empty(internal_queue)
