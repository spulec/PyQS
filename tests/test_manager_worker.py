import time
import boto

from mock import patch, Mock
from moto import mock_sqs

from pyqs.main import main, _main
from pyqs.worker import ManagerWorker


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

    ManagerWorker.assert_called_once_with(['email1', 'email2'], 2)
    ManagerWorker.return_value.start.assert_called_once_with()


@patch("pyqs.main._main")
@patch("pyqs.main.ArgumentParser")
@mock_sqs
def test_real_main_method(ArgumentParser, _main):
    ArgumentParser.return_value.parse_args.return_value = Mock(concurrency=3, queues=["email1"], logging_level="WARN")
    main()

    _main.assert_called_once_with(queue_prefixes=['email1'], concurrency=3, logging_level="WARN")


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
