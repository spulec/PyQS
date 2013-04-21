import boto
from mock import patch
from moto import mock_sqs

from pyqs.worker import ManagerWorker, main, _main


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
    sorted_children = sorted(children, key=lambda child: child.queue.name)

    sorted_children[0].queue.name.should.equal("email.baz")
    sorted_children[1].queue.name.should.equal("email.foobar")


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


@patch("pyqs.worker.ManagerWorker")
@mock_sqs
def test_main_method(ManagerWorker):
    _main(["email1", "email2"], concurrency=2)

    ManagerWorker.assert_called_once_with(['email1', 'email2'], 2)
    ManagerWorker.return_value.start.assert_called_once_with()


@patch("pyqs.worker._main")
@patch("pyqs.worker.OptionParser")
@mock_sqs
def test_real_main_method(OptionParser, _main):
    OptionParser.return_value.parse_args.return_value = ({'concurrency': 3}, 'email1')
    main()

    _main.assert_called_once_with('email1', concurrency=3)
