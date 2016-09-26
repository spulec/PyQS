import json
import logging
import time
import threading

from multiprocessing import Queue
from queue import Empty

import boto
from boto.sqs.message import Message
from moto import mock_sqs
from mock import patch, Mock
from pyqs.worker import ReadWorker, ProcessWorker, BaseWorker
from pyqs.utils import decode_message
from tests.tasks import task_results
from tests.utils import MockLoggingHandler


@mock_sqs
def test_worker_fills_internal_queue():
    """
    Test read workers fill internal queue
    """

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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")
    queue.set_timeout(1)  # Set visibility timeout low to improve test speed

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
    """
    Test read workers fill internal queue with celery tasks
    """
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    message = Message()
    body = '{"body": "KGRwMApTJ3Rhc2snCnAxClMndGVzdHMudGFza3MuaW5kZXhfaW5jcmVtZW50ZXInCnAyCnNTJ2Fy\\nZ3MnCnAzCihscDQKc1Mna3dhcmdzJwpwNQooZHA2ClMnbWVzc2FnZScKcDcKUydUZXN0IG1lc3Nh\\nZ2UyJwpwOApzcy4=\\n", "some stuff": "asdfasf"}'
    message.set_body(body)
    queue.write(message)

    internal_queue = Queue()
    worker = ReadWorker(queue, internal_queue)
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Build the SQS message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    }
    message = Message()
    body = json.dumps(message_body)
    message.set_body(body)

    # Add message to queue
    internal_queue = Queue()
    internal_queue.put({"message": message, "queue": queue.id, "start_time": time.time(), "timeout": 30})

    # Process message
    worker = ProcessWorker(internal_queue)
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")
    queue.set_timeout(1)

    # Add MEssages
    message = Message()
    body = '{"body": "KGRwMApTJ3Rhc2snCnAxClMndGVzdHMudGFza3MuaW5kZXhfaW5jcmVtZW50ZXInCnAyCnNTJ2Fy\\nZ3MnCnAzCihscDQKc1Mna3dhcmdzJwpwNQooZHA2ClMnbWVzc2FnZScKcDcKUydUZXN0IG1lc3Nh\\nZ2UyJwpwOApzcy4=\\n", "some stuff": "asdfasf"}'
    message.set_body(body)
    queue.write(message)
    queue.write(message)
    queue.write(message)

    # Run Reader
    internal_queue = Queue(maxsize=1)
    worker = ReadWorker(queue, internal_queue)
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Build the SQS message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 'Test message',
        },
    }
    message = Message()
    body = json.dumps(message_body)
    message.set_body(body)

    # Add message to internal queue
    internal_queue = Queue()
    internal_queue.put({"queue": queue.id, "message": message, "start_time": time.time(), "timeout": 30})

    # Process message
    worker = ProcessWorker(internal_queue)
    worker.process_message()

    # Check output
    expected_result = "Processed task tests.tasks.index_incrementer in 0.0000 seconds with args: [] and kwargs: {u'message': u'Test message'}"
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Build the SQS Message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }
    message = Message()
    body = json.dumps(message_body)
    message.set_body(body)

    # Add message to internal queue
    internal_queue = Queue()
    internal_queue.put({"queue": queue.id, "message": message, "start_time": time.time(), "timeout": 30})

    # Process message
    worker = ProcessWorker(internal_queue)
    worker.process_message()

    # Check output
    msg1 = "Task tests.tasks.index_incrementer raised error in 0.0000 seconds: with args: [] and kwargs: {u'message': 23}: Traceback (most recent call last)"  # noqa
    logger.handlers[0].messages['error'][0].lower().should.contain(msg1.lower())
    msg2 = 'raise ValueError("Need to be given basestring, was given {}".format(message))\nValueError: Need to be given basestring, was given 23'  # noqa
    logger.handlers[0].messages['error'][0].lower().should.contain(msg2.lower())


@mock_sqs
def test_worker_processes_empty_queue():
    """
    Test worker processes read from empty internal queue
    """
    internal_queue = Queue()

    worker = ProcessWorker(internal_queue)
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Setup PPID
    os.getppid.return_value = 1234

    # Setup dummy read_message
    def read_message():
        raise Exception("Called")

    # When I have a parent process, and shutdown is not set
    worker = ReadWorker(queue, "foo")
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Setup PPID
    os.getppid.return_value = 1234

    # When I have a parent process, and shutdown is set
    worker = ReadWorker(queue, "foo")
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Setup PPID
    os.getppid.return_value = 1

    # When I have no parent process, and shutdown is not set
    worker = ReadWorker(queue, "foo")
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
    worker = ProcessWorker("foo")
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
    worker = ProcessWorker("foo")
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
    worker = ProcessWorker("foo")
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Build the SQS Message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }
    message = Message()
    body = json.dumps(message_body)
    message.set_body(body)

    # Add message to internal queue
    internal_queue = Queue(3)
    internal_queue.put({"queue": queue.id, "message": message, "start_time": time.time(), "timeout": 30})
    internal_queue.put({"queue": queue.id, "message": message, "start_time": time.time(), "timeout": 30})
    internal_queue.put({"queue": queue.id, "message": message, "start_time": time.time(), "timeout": 30})

    # When I Process messages
    worker = ProcessWorker(internal_queue)
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
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Build the SQS Message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }
    message = Message()
    body = json.dumps(message_body)
    message.set_body(body)

    # Add message to internal queue with timeout of 0 that started long ago
    internal_queue = Queue()
    internal_queue.put({"queue": queue.id, "message": message, "start_time": 0, "timeout": 0})

    # When I process the message
    worker = ProcessWorker(internal_queue)
    worker.process_message()

    # Then I get an error about exceeding the visibility timeout
    msg1 = "Discarding task tests.tasks.index_incrementer with args: [] and kwargs: {u'message': 23} due to exceeding visibility timeout"  # noqa
    logger.handlers[0].messages['warning'][0].lower().should.contain(msg1.lower())


@mock_sqs
def test_worker_processes_only_increases_processed_counter_if_a_message_was_processed():
    """
    Test worker process only increases processed counter if a message was processed
    """
    # Setup SQS Queue
    conn = boto.connect_sqs()
    queue = conn.create_queue("tester")

    # Build the SQS Message
    message_body = {
        'task': 'tests.tasks.index_incrementer',
        'args': [],
        'kwargs': {
            'message': 23,
        },
    }
    message = Message()
    body = json.dumps(message_body)
    message.set_body(body)

    # Add message to internal queue
    internal_queue = Queue(3)
    internal_queue.put({"queue": queue.id, "message": message, "start_time": time.time(), "timeout": 30})

    # And we add a message to the queue later
    def sleep_and_queue(internal_queue):
        time.sleep(1)
        internal_queue.put({"queue": queue.id, "message": message, "start_time": time.time(), "timeout": 30})

    thread = threading.Thread(target=sleep_and_queue, args=(internal_queue,))
    thread.daemon = True
    thread.start()

    # When I Process messages
    worker = ProcessWorker(internal_queue)
    worker._messages_to_process_before_shutdown = 2

    # Then I return from run() after processing 2 messages
    worker.run().should.be.none
