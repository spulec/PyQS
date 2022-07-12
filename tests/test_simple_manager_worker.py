import json
import logging
import os
import signal
import time

import boto3
from mock import patch, Mock, MagicMock
from moto import mock_sqs

from pyqs.main import main, _main
from pyqs.worker import SimpleManagerWorker
from tests.utils import (
    MockLoggingHandler,
    ThreadWithReturnValue2,
    ThreadWithReturnValue3,
)


@mock_sqs
def test_simple_manager_worker_create_proper_children_workers():
    """
    Test simple managing process creates multiple child workers
    """
    conn = boto3.client("sqs", region_name="us-east-1")
    conn.create_queue(QueueName="email")

    manager = SimpleManagerWorker(
        queue_prefixes=["email"],
        worker_concurrency=3,
        interval=2,
        batchsize=10,
    )

    len(manager.worker_children).should.equal(3)


@mock_sqs
def test_simple_manager_worker_with_queue_prefix():
    """
    Test simple managing process can find queues by prefix
    """
    conn = boto3.client("sqs", region_name="us-east-1")
    conn.create_queue(QueueName="email.foobar")
    conn.create_queue(QueueName="email.baz")

    manager = SimpleManagerWorker(
        queue_prefixes=["email.*"],
        worker_concurrency=1,
        interval=1,
        batchsize=10,
    )

    len(manager.worker_children).should.equal(2)
    children = manager.worker_children
    # Pull all the read children and sort by name to make testing easier
    sorted_children = sorted(children, key=lambda child: child.queue_url)

    sorted_children[0].queue_url.should.equal(
        "https://queue.amazonaws.com/123456789012/email.baz"
    )
    sorted_children[1].queue_url.should.equal(
        "https://queue.amazonaws.com/123456789012/email.foobar"
    )


@mock_sqs
def test_simple_manager_start_and_stop():
    """
    Test simple managing process can start and stop child processes
    """
    conn = boto3.client("sqs", region_name="us-east-1")
    conn.create_queue(QueueName="email")

    manager = SimpleManagerWorker(
        queue_prefixes=["email"],
        worker_concurrency=2,
        interval=1,
        batchsize=10,
    )

    len(manager.worker_children).should.equal(2)

    manager.worker_children[0].is_alive().should.equal(False)
    manager.worker_children[1].is_alive().should.equal(False)

    manager.start()

    manager.worker_children[0].is_alive().should.equal(True)
    manager.worker_children[1].is_alive().should.equal(True)

    manager.stop()

    manager.worker_children[0].is_alive().should.equal(False)
    manager.worker_children[1].is_alive().should.equal(False)


@patch("pyqs.main.SimpleManagerWorker")
@mock_sqs
def test_main_method(SimpleManagerWorker):
    """
    Test creation of simple manager process from _main method
    """
    _main(["email1", "email2"], concurrency=2, simple_worker=True)

    SimpleManagerWorker.assert_called_once_with(
        ["email1", "email2"],
        2,
        1,
        10,
        region=None,
        secret_access_key=None,
        access_key_id=None,
        endpoint_url=None,
    )
    SimpleManagerWorker.return_value.start.assert_called_once_with()


@patch("pyqs.main._main")
@patch("pyqs.main.ArgumentParser")
@mock_sqs
def test_real_main_method(ArgumentParser, _main):
    """
    Test parsing of arguments from main method
    """
    ArgumentParser.return_value.parse_args.return_value = Mock(
        concurrency=3,
        queues=["email1"],
        interval=1,
        batchsize=5,
        logging_level="WARN",
        region="us-east-1",
        prefetch_multiplier=2,
        access_key_id=None,
        secret_access_key=None,
        simple_worker=True,
        endpoint_url=None,
    )
    main()

    _main.assert_called_once_with(
        queue_prefixes=["email1"],
        concurrency=3,
        interval=1,
        batchsize=5,
        logging_level="WARN",
        region="us-east-1",
        prefetch_multiplier=2,
        access_key_id=None,
        secret_access_key=None,
        simple_worker=True,
        endpoint_url=None,
    )


@patch("pyqs.main._main")
@patch("pyqs.main.ArgumentParser")
@mock_sqs
def test_real_main_method_default_batchsize(ArgumentParser, _main):
    """
    Test parsing of arguments from main method batch default
    """
    ArgumentParser.return_value.parse_args.return_value = Mock(
        concurrency=3,
        queues=["email1"],
        interval=1,
        batchsize=None,
        logging_level="WARN",
        region="us-east-1",
        prefetch_multiplier=2,
        access_key_id=None,
        secret_access_key=None,
        simple_worker=True,
        endpoint_url=None,
    )
    main()

    _main.assert_called_once_with(
        queue_prefixes=["email1"],
        concurrency=3,
        interval=1,
        batchsize=1,
        logging_level="WARN",
        region="us-east-1",
        prefetch_multiplier=2,
        access_key_id=None,
        secret_access_key=None,
        simple_worker=True,
        endpoint_url=None,
    )


@mock_sqs
def test_master_spawns_worker_processes():
    """
    Test simple managing process creates child workers
    """

    # Setup SQS Queue
    conn = boto3.client("sqs", region_name="us-east-1")
    conn.create_queue(QueueName="tester")

    # Setup Manager
    manager = SimpleManagerWorker(["tester"], 1, 1, 10)
    manager.start()

    # Check Workers
    len(manager.worker_children).should.equal(1)

    manager.worker_children[0].is_alive().should.be.true

    # Cleanup
    manager.stop()


@mock_sqs
def test_master_counts_processes():
    """
    Test simple managing process counts child processes
    """

    # Setup Logging
    logger = logging.getLogger("pyqs")
    del logger.handlers[:]
    logger.handlers.append(MockLoggingHandler())

    # Setup SQS Queue
    conn = boto3.client("sqs", region_name="us-east-1")
    conn.create_queue(QueueName="tester")

    # Setup Manager
    manager = SimpleManagerWorker(["tester"], 2, 1, 10)
    manager.start()

    # Check Workers
    manager.process_counts()

    # Cleanup
    manager.stop()

    # Check messages
    msg2 = "Worker Processes: 2"
    logger.handlers[0].messages["debug"][-1].lower().should.contain(msg2.lower())


@mock_sqs
def test_master_replaces_worker_processes():
    """
    Test simple managing process replaces worker processes
    """
    # Setup SQS Queue
    conn = boto3.client("sqs", region_name="us-east-1")
    conn.create_queue(QueueName="tester")

    # Setup Manager
    manager = SimpleManagerWorker(
        queue_prefixes=["tester"],
        worker_concurrency=1,
        interval=1,
        batchsize=10,
    )
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
    Test simple managing process handles OS signals
    """

    # Setup SQS Queue
    conn = boto3.client("sqs", region_name="us-east-1")
    conn.create_queue(QueueName="tester")

    # Mock out sys.exit
    sys.exit = Mock()

    # Have our inner method send our signal
    def process_counts():
        os.kill(os.getpid(), signal.SIGTERM)

    # Setup Manager
    manager = SimpleManagerWorker(
        queue_prefixes=["tester"],
        worker_concurrency=1,
        interval=1,
        batchsize=10,
    )
    manager.process_counts = process_counts
    manager._graceful_shutdown = MagicMock()

    # When we start and trigger a signal
    manager.start()
    manager.sleep()

    # Then we exit
    sys.exit.assert_called_once_with(0)


@mock_sqs
def test_master_shuts_down_busy_process_workers():
    """
    Test simple managing process properly cleans up busy Process Workers
    """
    # For debugging test
    import sys

    logger = logging.getLogger("pyqs")
    logger.setLevel(logging.DEBUG)
    stdout_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stdout_handler)

    # Setup SQS Queue
    conn = boto3.client("sqs", region_name="us-east-1")
    queue_url = conn.create_queue(QueueName="tester")["QueueUrl"]

    # Add Slow tasks
    message = json.dumps(
        {
            "task": "tests.tasks.sleeper",
            "args": [],
            "kwargs": {
                "message": 5,
            },
        }
    )

    # Fill the queue (we need a lot of messages to trigger the bug)
    for _ in range(20):
        conn.send_message(QueueUrl=queue_url, MessageBody=message)

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
    manager = SimpleManagerWorker(
        queue_prefixes=["tester"],
        worker_concurrency=1,
        interval=0.0,
        batchsize=1,
    )
    manager.start()

    # Give our processes a moment to start
    time.sleep(1)

    # Setup Threading watcher
    try:
        # Try Python 2 Style
        thread = ThreadWithReturnValue2(
            target=sleep_and_kill, args=(manager.worker_children[0].pid,)
        )
        thread.daemon = True
    except TypeError:
        # Use Python 3 Style
        thread = ThreadWithReturnValue3(
            target=sleep_and_kill,
            args=(manager.worker_children[0].pid,),
            daemon=True,
        )

    thread.start()

    # Stop the Master Process
    manager.stop()

    # Check if we had to kill the Process Worker or it exited gracefully
    return_value = thread.join()
    if not return_value:
        raise Exception("Process Worker failed to quit!")


@mock_sqs
def test_manager_picks_up_new_queues():
    """
    Test that the simple manager will recognize new SQS queues have been added
    """

    # Setup SQS Queue
    conn = boto3.client("sqs", region_name="us-east-1")

    # Setup Manager
    manager = SimpleManagerWorker(
        queue_prefixes=["tester"],
        worker_concurrency=1,
        interval=1,
        batchsize=10,
    )
    manager.start()

    # No queues found
    len(manager.worker_children).should.equal(0)

    # Create the queue
    conn.create_queue(QueueName="tester")
    manager.check_for_new_queues()

    # The manager should have seen the new queue was created and add a reader
    len(manager.worker_children).should.equal(1)
    manager.worker_children[0].queue_url.should.equal(
        "https://queue.amazonaws.com/123456789012/tester"
    )

    # Cleanup
    manager.stop()
