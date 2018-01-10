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
from pyqs.worker import ManagerWorker, ReadWorker, ProcessWorker, BaseWorker, MESSAGE_DOWNLOAD_BATCH_SIZE
from pyqs.utils import decode_message
from tests.tasks import task_results
from tests.utils import MockLoggingHandler, Struct

BATCHSIZE = 10
INTERVAL = 0.1


@mock_sqs
def test_worker_fills_internal_queue():
    """
    Test read workers fill internal queue
    """
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    body = json.dumps({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })
    queue.send_message(
        MessageBody=body
    )

    internal_queue = Queue()
    worker = ReadWorker(queue, internal_queue, BATCHSIZE)
    worker.read_message()

    packed_message = internal_queue.get(timeout=1)
    print(packed_message)

    class Struct:
        def __init__(self, foo):
            self.__dict__ = foo

    found_message_body = decode_message(Struct({'body': json.dumps(packed_message['message'])}))
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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])
    queue.set_attributes(Attributes={'VisibilityTimeout': '1'})  # Set visibility timeout low to improve test speed

    body = json.dumps({
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    })
    for i in range(3):
        queue.send_message(MessageBody=body)

    internal_queue = Queue(maxsize=2)
    worker = ReadWorker(queue, internal_queue, BATCHSIZE)
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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    body = '{"body": "KGRwMApTJ3Rhc2snCnAxClMndGVzdHMudGFza3MuaW5kZXhfaW5jcmVtZW50ZXInCnAyCnNTJ2Fy\\nZ3MnCnAzCihscDQKc1Mna3dhcmdzJwpwNQooZHA2ClMnbWVzc2FnZScKcDcKUydUZXN0IG1lc3Nh\\nZ2UyJwpwOApzcy4=\\n", "some stuff": "asdfasf"}'
    queue.send_message(MessageBody=body)

    internal_queue = Queue()
    worker = ReadWorker(queue, internal_queue, BATCHSIZE)
    worker.read_message()

    packed_message = internal_queue.get(timeout=1)
    found_message_body = decode_message(Struct({'body': json.dumps(packed_message['message'])}))
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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Build the SQS message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    }

    # Add message to queue
    internal_queue = Queue()
    internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": time.time(), "timeout": 30, "receipt_handle": "foobar"})

    # Process message
    worker = ProcessWorker(internal_queue, INTERVAL)
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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])
    queue.set_attributes(Attributes={'VisibilityTimeout': '1'})

    # Add MEssages
    body = '{"body": "KGRwMApTJ3Rhc2snCnAxClMndGVzdHMudGFza3MuaW5kZXhfaW5jcmVtZW50ZXInCnAyCnNTJ2Fy\\nZ3MnCnAzCihscDQKc1Mna3dhcmdzJwpwNQooZHA2ClMnbWVzc2FnZScKcDcKUydUZXN0IG1lc3Nh\\nZ2UyJwpwOApzcy4=\\n", "some stuff": "asdfasf"}'
    queue.send_message(MessageBody=body)
    queue.send_message(MessageBody=body)
    queue.send_message(MessageBody=body)

    # Run Reader
    internal_queue = Queue(maxsize=1)
    worker = ReadWorker(queue, internal_queue, BATCHSIZE)
    worker.read_message()

    # Check log messages
    logger.handlers[0].messages['warning'][0].should.contain("Timed out trying to add the following message to the internal queue")
    logger.handlers[0].messages['warning'][1].should.contain("Clearing Local messages since we exceeded their visibility_timeout")


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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Build the SQS message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    }

    # Add message to internal queue
    internal_queue = Queue()
    internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": time.time(), "timeout": 30, "receipt_handle": "foobar"})

    # Process message
    worker = ProcessWorker(internal_queue, INTERVAL)
    worker.process_message()

    # Check output
    kwargs = message_body['kwargs']
    expected_result = u"Processed task tests.tasks.index_incrementer in 0.0000 seconds with args: [] and kwargs: {}".format(kwargs)
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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Build the SQS Message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }

    # Add message to internal queue
    internal_queue = Queue()
    internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": time.time(), "timeout": 30, "receipt_handle": "foobar"})

    # Process message
    worker = ProcessWorker(internal_queue, INTERVAL)
    worker.process_message()

    # Check output
    kwargs = message_body['kwargs']
    msg1 = "Task tests.tasks.index_incrementer raised error in 0.0000 seconds: with args: [] and kwargs: {}: Traceback (most recent call last)".format(kwargs)  # noqa
    logger.handlers[0].messages['error'][0].lower().should.contain(msg1.lower())
    msg2 = 'raise ValueError("Need to be given basestring, was given {}".format(message))\nValueError: Need to be given basestring, was given 23'  # noqa
    logger.handlers[0].messages['error'][0].lower().should.contain(msg2.lower())


@mock_sqs
def test_worker_processes_empty_queue():
    """
    Test worker processes read from empty internal queue
    """
    internal_queue = Queue()

    worker = ProcessWorker(internal_queue, INTERVAL)
    worker.process_message()


@patch("pyqs.worker.os")
def test_parent_process_death(os):
    """
    Test worker processes recognize parent process death
    """
    os.getppid.return_value = 1

    worker = BaseWorker()
    worker.parent_is_alive().should.be.false


@patch("pyqs.worker.os")
def test_parent_process_alive(os):
    """
    Test worker processes recognize when parent process is alive
    """
    os.getppid.return_value = 1234

    worker = BaseWorker()
    worker.parent_is_alive().should.be.true


@mock_sqs
@patch("pyqs.worker.os")
def test_read_worker_with_parent_process_alive_and_should_not_exit(os):
    """
    Test read workers do not exit when parent is alive and shutdown is not set
    """
    # Setup SQS Queue
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Setup PPID
    os.getppid.return_value = 1234

    # Setup dummy read_message
    def read_message():
        raise Exception("Called")

    # When I have a parent process, and shutdown is not set
    worker = ReadWorker(queue, "foo", BATCHSIZE)
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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Setup PPID
    os.getppid.return_value = 1234

    # Setup internal queue
    q = Queue(1)

    # When I have a parent process, and shutdown is set
    worker = ReadWorker(queue, q, BATCHSIZE)
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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Setup PPID
    os.getppid.return_value = 1

    # Setup internal queue
    q = Queue(1)

    # When I have no parent process, and shutdown is not set
    worker = ReadWorker(queue, q, BATCHSIZE)
    worker.read_message = Mock()

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
@patch("pyqs.worker.os")
def test_process_worker_with_parent_process_alive_and_should_not_exit(os):
    """
    Test worker processes do not exit when parent is alive and shutdown is not set
    """
    # Setup PPID
    os.getppid.return_value = 1234

    # Setup dummy read_message
    def process_message():
        raise Exception("Called")

    # When I have a parent process, and shutdown is not set
    worker = ProcessWorker("foo", INTERVAL)
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
    worker = ProcessWorker("foo", INTERVAL)
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
    worker = ProcessWorker("foo", INTERVAL)
    worker.process_message = Mock()
    worker.shutdown()

    # Then I return from run()
    worker.run().should.be.none


@mock_sqs
def test_worker_processes_shuts_down_after_processing_its_maximum_number_of_messages():
    """
    Test worker processes shutdown after processing maximum number of messages
    """
    # Setup SQS Queue
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Build the SQS Message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }

    # Add message to internal queue
    internal_queue = Queue(3)
    internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": time.time(), "timeout": 30, "receipt_handle": None})
    internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": time.time(), "timeout": 30, "receipt_handle": None})
    internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": time.time(), "timeout": 30, "receipt_handle": None})

    # When I Process messages
    worker = ProcessWorker(internal_queue, INTERVAL)
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
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Build the SQS Message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }

    # Add message to internal queue with timeout of 0 that started long ago
    internal_queue = Queue()
    internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": 0, "timeout": 0, "receipt_handle": None})

    # When I process the message
    worker = ProcessWorker(internal_queue, INTERVAL)
    worker.process_message()

    # Then I get an error about exceeding the visibility timeout
    kwargs = message_body['kwargs']
    msg1 = "Discarding task tests.tasks.index_incrementer with args: [] and kwargs: {} due to exceeding visibility timeout".format(kwargs)  # noqa
    logger.handlers[0].messages['warning'][0].lower().should.contain(msg1.lower())


@mock_sqs
def test_worker_processes_only_increases_processed_counter_if_a_message_was_processed():
    """
    Test worker process only increases processed counter if a message was processed
    """
    # Setup SQS Queue
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Build the SQS Message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }

    # Add message to internal queue
    internal_queue = Queue(3)
    internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": time.time(), "timeout": 30, "receipt_handle": None})

    # And we add a message to the queue later
    def sleep_and_queue(internal_queue):
        time.sleep(1)
        internal_queue.put({"queue_url": queue.url, "message": message_body, "start_time": time.time(), "timeout": 30, "receipt_handle": None})

    thread = threading.Thread(target=sleep_and_queue, args=(internal_queue,))
    thread.daemon = True
    thread.start()

    # When I Process messages
    worker = ProcessWorker(internal_queue, INTERVAL)
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
    client = boto3.client('sqs')
    client.create_queue(QueueName="tester")

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
    client = boto3.client('sqs')
    client.create_queue(QueueName="tester")

    worker = ManagerWorker(QUEUE_PREFIX, CONCURRENCY, INTERVAL, BATCHSIZE)
    worker.batchsize.should.equal(MESSAGE_DOWNLOAD_BATCH_SIZE)
