import os
import signal
import time
import boto
import logging

from mock import patch, Mock, MagicMock
from moto import mock_sqs

from pyqs.main import main, _main
from pyqs.worker import ManagerWorker, _get_region
from tests.utils import MockLoggingHandler


@mock_sqs
def test_manager_worker_create_proper_children_workers():
    conn = boto.connect_sqs()
    conn.create_queue("email")

    manager = ManagerWorker(queue_prefixes=['email'], worker_concurrency=3)

    len(manager.reader_children).should.equal(1)
    len(manager.worker_children).should.equal(3)


@mock_sqs
def test_manager_worker_with_queue_prefix():
    conn = boto.connect_sqs()
    conn.create_queue("email.foobar")
    conn.create_queue("email.baz")

    manager = ManagerWorker(queue_prefixes=['email.*'], worker_concurrency=1)

    len(manager.reader_children).should.equal(2)
    children = manager.reader_children
    # Pull all the read children and sort by name to make testing easier
    sorted_children = sorted(children, key=lambda child: child.sqs_queue.name)

    sorted_children[0].sqs_queue.name.should.equal("email.baz")
    sorted_children[1].sqs_queue.name.should.equal("email.foobar")


@mock_sqs
def test_manager_start_and_stop():
    conn = boto.connect_sqs()
    conn.create_queue("email")

    manager = ManagerWorker(queue_prefixes=['email'], worker_concurrency=2)

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
    _main(["email1", "email2"], concurrency=2)

    ManagerWorker.assert_called_once_with(['email1', 'email2'], 2, region='us-east-1', secret_access_key=None, access_key_id=None)
    ManagerWorker.return_value.start.assert_called_once_with()


@patch("pyqs.main._main")
@patch("pyqs.main.ArgumentParser")
@mock_sqs
def test_real_main_method(ArgumentParser, _main):
    ArgumentParser.return_value.parse_args.return_value = Mock(concurrency=3,
                                                               queues=["email1"],
                                                               logging_level="WARN",
                                                               region='us-east-1',
                                                               access_key_id=None,
                                                               secret_access_key=None)
    main()

    _main.assert_called_once_with(queue_prefixes=['email1'],
                                  concurrency=3,
                                  logging_level="WARN",
                                  region='us-east-1',
                                  access_key_id=None,
                                  secret_access_key=None)


@mock_sqs
def test_master_spawns_worker_processes():
    # Setup SQS Queue
    conn = boto.connect_sqs()
    conn.create_queue("tester")

    # Setup Manager
    manager = ManagerWorker(["tester"], 1)
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
    # Setup SQS Queue
    conn = boto.connect_sqs()
    conn.create_queue("tester")

    # Setup Manager
    manager = ManagerWorker(queue_prefixes=["tester"], worker_concurrency=1)
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
    # Setup Logging
    logger = logging.getLogger("pyqs")
    del logger.handlers[:]
    logger.handlers.append(MockLoggingHandler())

    # Setup SQS Queue
    conn = boto.connect_sqs()
    conn.create_queue("tester")

    # Setup Manager
    manager = ManagerWorker(["tester"], 2)
    manager.start()

    # Check Workers
    manager.process_counts()

    # Cleanup
    manager.stop()

    # Check messages
    msg1 = "Reader Processes: 1"
    logger.handlers[0].messages['info'][-2].lower().should.contain(msg1.lower())
    msg2 = "Worker Processes: 2"
    logger.handlers[0].messages['info'][-1].lower().should.contain(msg2.lower())


@mock_sqs
def test_master_replaces_worker_processes():
    # Setup SQS Queue
    conn = boto.connect_sqs()
    conn.create_queue("tester")

    # Setup Manager
    manager = ManagerWorker(queue_prefixes=["tester"], worker_concurrency=1)
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
    # Setup SQS Queue
    conn = boto.connect_sqs()
    conn.create_queue("tester")

    # Mock out sys.exit
    sys.exit = Mock()

    # Have our inner method send our signal
    def process_counts():
        os.kill(os.getpid(), signal.SIGTERM)

    # Setup Manager
    manager = ManagerWorker(queue_prefixes=["tester"], worker_concurrency=1)
    manager.process_counts = process_counts
    manager._graceful_shutdown = MagicMock()

    # When we start and trigger a signal
    manager.start()
    manager.sleep()

    # Then we exit
    sys.exit.assert_called_once_with(0)


def test_region_from_string_that_exists():
    region_name = 'us-east-1'

    region = _get_region(region_name)
    region.shouldnt.be.none


def test_region_from_string_that_does_not_exist():
    region_name = 'foobar'

    region = _get_region(region_name)
    region.should.be.none


def test_region_from_string_that_is_none():
    region_name = None

    region = _get_region(region_name)
    region.should.be.none
