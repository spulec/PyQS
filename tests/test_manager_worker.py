import boto3
import json
import logging
import os
import signal
import time

from mock import patch, Mock, MagicMock
from moto import mock_sqs

from pyqs.main import main, _main
from pyqs.worker import ManagerWorker
from tests.utils import MockLoggingHandler, ThreadWithReturnValue2, ThreadWithReturnValue3


@mock_sqs
def test_manager_worker_create_proper_children_workers():
    """
    Test managing process creates multiple child workers
    """
    client = boto3.client('sqs')
    client.create_queue(QueueName="email")

    manager = ManagerWorker(queue_prefixes=['email'], worker_concurrency=3, interval=2, batchsize=10)

    len(manager.reader_children).should.equal(1)
    len(manager.worker_children).should.equal(3)


@mock_sqs
def test_manager_worker_with_queue_prefix():
    """
    Test managing process can find queues by prefix
    """
    client = boto3.client('sqs')
    client.create_queue(QueueName="email_foobar")
    client.create_queue(QueueName="email_baz")

    manager = ManagerWorker(queue_prefixes=['email.*'], worker_concurrency=1, interval=1, batchsize=10)

    len(manager.reader_children).should.equal(2)
    children = manager.reader_children
    # Pull all the read children and sort by name to make testing easier
    sorted_children = sorted(children, key=lambda child: child.sqs_queue.url)

    sorted_children[0].sqs_queue.url.should.contain("email_baz")
    sorted_children[1].sqs_queue.url.should.contain("email_foobar")


@mock_sqs
def test_manager_start_and_stop():
    """
    Test managing process can start and stop child processes
    """
    client = boto3.client('sqs')
    client.create_queue(QueueName="email")

    manager = ManagerWorker(queue_prefixes=['email'], worker_concurrency=2, interval=1, batchsize=10)

    len(manager.worker_children).should.equal(2)

    manager.worker_children[0].is_alive().should.equal(False)
    manager.worker_children[1].is_alive().should.equal(False)

    manager.start()

    manager.worker_children[0].is_alive().should.equal(True)
    manager.worker_children[1].is_alive().should.equal(True)

    manager.stop()

    manager.worker_children[0].is_alive().should.equal(False)
    manager.worker_children[1].is_alive().should.equal(False)


@patch("pyqs.main.ManagerWorker")
@mock_sqs
def test_main_method(ManagerWorker):
    """
    Test creation of manager process from _main method
    """
    _main(["email1", "email2"], concurrency=2)

    ManagerWorker.assert_called_once_with(['email1', 'email2'], 2, 1, 10, prefetch_multiplier=2)
    ManagerWorker.return_value.start.assert_called_once_with()


@patch("pyqs.main._main")
@patch("pyqs.main.ArgumentParser")
@mock_sqs
def test_real_main_method(ArgumentParser, _main):
    """
    Test parsing of arguments from main method
    """
    ArgumentParser.return_value.parse_args.return_value = Mock(concurrency=3,
                                                               queues=["email1"],
                                                               interval=1,
                                                               batchsize=10,
                                                               logging_level="WARN",
                                                               prefetch_multiplier=2)
    main()

    _main.assert_called_once_with(queue_prefixes=['email1'],
                                  concurrency=3,
                                  interval=1,
                                  batchsize=10,
                                  logging_level="WARN",
                                  prefetch_multiplier=2)


@mock_sqs
def test_master_spawns_worker_processes():
    """
    Test managing process creates child workers
    """

    # Setup SQS Queue
    client = boto3.client('sqs')
    client.create_queue(QueueName="tester")

    # Setup Manager
    manager = ManagerWorker(["tester"], 1, 1, 10)
    manager.start()

    # Check Workers
    len(manager.reader_children).should.equal(1)
    len(manager.worker_children).should.equal(1)

    manager.reader_children[0].is_alive().should.be.true
    manager.worker_children[0].is_alive().should.be.true

    # Cleanup
    manager.stop()


@mock_sqs
def test_master_replaces_reader_processes():
    """
    Test managing process replaces reader children
    """

    # Setup SQS Queue
    client = boto3.client('sqs')
    client.create_queue(QueueName="tester")

    # Setup Manager
    manager = ManagerWorker(queue_prefixes=["tester"], worker_concurrency=1, interval=1, batchsize=10)
    manager.start()

    # Get Reader PID
    pid = manager.reader_children[0].pid

    # Kill Reader and wait to replace
    manager.reader_children[0].shutdown()
    time.sleep(0.1)
    manager.replace_workers()

    # Check Replacement
    manager.reader_children[0].pid.shouldnt.equal(pid)

    # Cleanup
    manager.stop()


@mock_sqs
def test_master_counts_processes():
    """
    Test managing process counts child processes
    """

    # Setup Logging
    logger = logging.getLogger("pyqs")
    del logger.handlers[:]
    logger.handlers.append(MockLoggingHandler())

    # Setup SQS Queue
    client = boto3.client('sqs')
    client.create_queue(QueueName="tester")

    # Setup Manager
    manager = ManagerWorker(["tester"], 2, 1, 10)
    manager.start()

    # Check Workers
    manager.process_counts()

    # Cleanup
    manager.stop()

    # Check messages
    msg1 = "Reader Processes: 1"
    logger.handlers[0].messages['debug'][-2].lower().should.contain(msg1.lower())
    msg2 = "Worker Processes: 2"
    logger.handlers[0].messages['debug'][-1].lower().should.contain(msg2.lower())


@mock_sqs
def test_master_replaces_worker_processes():
    """
    Test managing process replaces worker processes
    """
    # Setup SQS Queue
    client = boto3.client('sqs')
    client.create_queue(QueueName="tester")

    # Setup Manager
    manager = ManagerWorker(queue_prefixes=["tester"], worker_concurrency=1, interval=1, batchsize=10)
    manager.start()

    # Get Worker PID
    pid = manager.worker_children[0].pid

    # Kill Worker and wait to replace
    manager.worker_children[0].shutdown()
    time.sleep(0.1)
    manager.replace_workers()

    # Check Replacement
    manager.worker_children[0].pid.shouldnt.equal(pid)

    # Cleanup
    manager.stop()


@mock_sqs
@patch("pyqs.worker.sys")
def test_master_handles_signals(sys):
    """
    Test managing process handles OS signals
    """

    # Setup SQS Queue
    client = boto3.client('sqs')
    client.create_queue(QueueName="tester")

    # Mock out sys.exit
    sys.exit = Mock()

    # Have our inner method send our signal
    def process_counts():
        os.kill(os.getpid(), signal.SIGTERM)

    # Setup Manager
    manager = ManagerWorker(queue_prefixes=["tester"], worker_concurrency=1, interval=1, batchsize=10)
    manager.process_counts = process_counts
    manager._graceful_shutdown = MagicMock()

    # When we start and trigger a signal
    manager.start()
    manager.sleep()

    # Then we exit
    sys.exit.assert_called_once_with(0)


@mock_sqs
def test_master_shuts_down_busy_read_workers():
    """
    Test managing process properly cleans up busy Reader Workers
    """
    # For debugging test
    import sys
    logger = logging.getLogger("pyqs")
    logger.setLevel(logging.DEBUG)
    stdout_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stdout_handler)

    # Setup SQS Queue
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Add Slow tasks
    body = json.dumps({
        'task': 'tests.tasks.sleeper',
        'args': [],
        'kwargs': {
            'message': 5,
        },
    })

    # Fill the queue (we need a lot of messages to trigger the bug)
    for _ in range(20):
        queue.send_message(
            MessageBody=body
        )

    # Create function to watch and kill stuck processes
    def sleep_and_kill(pid):
        import os
        import signal
        import time
        # This sleep time is long enoug for 100 messages in queue
        time.sleep(5)
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            # Return that we didn't need to kill the process
            return True
        else:
            # Return that we needed to kill the process
            return False

    # Setup Manager
    manager = ManagerWorker(queue_prefixes=["tester"], worker_concurrency=0, interval=0.0, batchsize=1)
    manager.start()

    # Give our processes a moment to start
    time.sleep(1)

    # Setup Threading watcher
    try:
        # Try Python 2 Style
        thread = ThreadWithReturnValue2(target=sleep_and_kill, args=(manager.reader_children[0].pid,))
        thread.daemon = True
    except TypeError:
        # Use Python 3 Style
        thread = ThreadWithReturnValue3(target=sleep_and_kill, args=(manager.reader_children[0].pid,), daemon=True)

    thread.start()

    # Stop the Master Process
    manager.stop()

    # Check if we had to kill the Reader Worker or it exited gracefully
    return_value = thread.join()
    if not return_value:
        raise Exception("Reader Worker failed to quit!")


@mock_sqs
def test_master_shuts_down_busy_process_workers():
    """
    Test managing process properly cleans up busy Process Workers
    """
    # For debugging test
    import sys
    logger = logging.getLogger("pyqs")
    logger.setLevel(logging.DEBUG)
    stdout_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stdout_handler)

    # Setup SQS Queue
    client = boto3.client('sqs')
    resp = client.create_queue(QueueName="tester")
    queue = boto3.resource('sqs').Queue(resp['QueueUrl'])

    # Add Slow tasks
    body = json.dumps({
        'task': 'tests.tasks.sleeper',
        'args': [],
        'kwargs': {
            'message': 5,
        },
    })

    # Fill the queue (we need a lot of messages to trigger the bug)
    for _ in range(20):
        queue.send_message(
            MessageBody=body
        )

    # Create function to watch and kill stuck processes
    def sleep_and_kill(pid):
        import os
        import signal
        import time
        # This sleep time is long enoug for 100 messages in queue
        time.sleep(5)
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            # Return that we didn't need to kill the process
            return True
        else:
            # Return that we needed to kill the process
            return False

    # Setup Manager
    manager = ManagerWorker(queue_prefixes=["tester"], worker_concurrency=1, interval=0.0, batchsize=1)
    manager.start()

    # Give our processes a moment to start
    time.sleep(1)

    # Setup Threading watcher
    try:
        # Try Python 2 Style
        thread = ThreadWithReturnValue2(target=sleep_and_kill, args=(manager.reader_children[0].pid,))
        thread.daemon = True
    except TypeError:
        # Use Python 3 Style
        thread = ThreadWithReturnValue3(target=sleep_and_kill, args=(manager.reader_children[0].pid,), daemon=True)

    thread.start()

    # Stop the Master Process
    manager.stop()

    # Check if we had to kill the Reader Worker or it exited gracefully
    return_value = thread.join()
    if not return_value:
        raise Exception("Reader Worker failed to quit!")
