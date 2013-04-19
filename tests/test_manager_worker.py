import os

import boto
from moto import mock_sqs
import sure  # flake8: noqa

from pyqs.worker import ManagerWorker


@mock_sqs
def test_manager_worker_create_proper_children_workers():
    conn = boto.connect_sqs()
    queue = conn.create_queue("email")

    manager = ManagerWorker(queue_prefix='email', worker_concurrency=3)

    len(manager.reader_children).should.equal(1)
    len(manager.worker_children).should.equal(3)


@mock_sqs
def test_manager_worker_with_queue_prefix():
    conn = boto.connect_sqs()
    queue = conn.create_queue("email.foobar")
    queue = conn.create_queue("email.baz")

    manager = ManagerWorker(queue_prefix='email.*')

    len(manager.reader_children).should.equal(2)
    children = manager.reader_children
    # Pull all the read children and sort by name to make testing easier
    sorted_children = sorted(children, key=lambda child: child.queue.name)

    sorted_children[0].queue.name.should.equal("email.baz")
    sorted_children[1].queue.name.should.equal("email.foobar")


@mock_sqs
def test_manager_start_and_stop():
    conn = boto.connect_sqs()
    queue = conn.create_queue("email")

    manager = ManagerWorker(queue_prefix='email', worker_concurrency=2)

    len(manager.worker_children).should.equal(2)
    curr_pid = os.getpid()

    manager.worker_children[0].is_alive().should.equal(False)
    manager.worker_children[1].is_alive().should.equal(False)

    manager.start()

    manager.worker_children[0].is_alive().should.equal(True)
    manager.worker_children[1].is_alive().should.equal(True)

    manager.stop()

    manager.worker_children[0].is_alive().should.equal(False)
    manager.worker_children[1].is_alive().should.equal(False)
