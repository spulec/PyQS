# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import fnmatch
import importlib
import logging
import os
import signal
import sys
import traceback
import time

from multiprocessing import Event, Process, Queue
from Queue import Empty, Full

import boto

from pyqs.utils import decode_message

PREFETCH_MULTIPLIER = 2
MESSAGE_DOWNLOAD_BATCH_SIZE = 10
logger = logging.getLogger("pyqs")
conn = None


def get_conn():
    # TODO clean this up
    global conn
    if conn:
        return conn
    else:
        conn = boto.connect_sqs()
        return conn


class BaseWorker(Process):
    def __init__(self, *args, **kwargs):
        super(BaseWorker, self).__init__(*args, **kwargs)
        self.should_exit = Event()

    def shutdown(self):
        self.should_exit.set()

    def parent_is_alive(self):
        if os.getppid() == 1:
            logger.info("Parent process has gone away, exiting process {}!".format(os.getpid()))
            return False
        return True


class ReadWorker(BaseWorker):

    def __init__(self, sqs_queue, internal_queue, *args, **kwargs):
        super(ReadWorker, self).__init__(*args, **kwargs)
        self.sqs_queue = sqs_queue
        self.visibility_timeout = self.sqs_queue.get_timeout()
        self.internal_queue = internal_queue

    def run(self):
        # Set the child process to not receive any keyboard interrupts
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        logger.info("Running ReadWorker: {}, pid: {}".format(self.sqs_queue.name, os.getpid()))
        while not self.should_exit.is_set() and self.parent_is_alive():
            self.read_message()

    def read_message(self):
        messages = self.sqs_queue.get_messages(MESSAGE_DOWNLOAD_BATCH_SIZE)
        start = time.time()
        for message in messages:
            end = time.time()
            if int(end - start) >= self.visibility_timeout:
                # Don't add any more messages since they have re-appeared in the sqs queue
                # Instead just reset and get fresh messages from the sqs queue
                msg = "Clearing Local messages since we exceeded their visibility_timeout"
                logger.warning(msg)
                break

            message_body = decode_message(message)
            try:
                self.internal_queue.put(message_body, True, self.visibility_timeout)
            except Full:
                msg = "Timed out trying to add the following message to the internal queue after {} seconds: {}".format(self.visibility_timeout, message_body)  # noqa
                logger.warning(msg)
                continue
            else:
                logger.debug("Message successfully added to internal queue, deleting from SQS queue {} with body: ".format(self.sqs_queue.name, message_body))  # noqa
                message.delete()


class ProcessWorker(BaseWorker):

    def __init__(self, internal_queue, *args, **kwargs):
        super(ProcessWorker, self).__init__(*args, **kwargs)
        self.internal_queue = internal_queue

    def run(self):
        # Set the child process to not receive any keyboard interrupts
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        logger.info("Running ProcessWorker, pid: {}".format(os.getpid()))
        while not self.should_exit.is_set() and self.parent_is_alive():
            self.process_message()

    def process_message(self):
        try:
            next_message = self.internal_queue.get(timeout=0.5)
        except Empty:
            return

        full_task_path = next_message['task']
        args = next_message['args']
        kwargs = next_message['kwargs']

        task_name = full_task_path.split(".")[-1]
        task_path = ".".join(full_task_path.split(".")[:-1])

        task_module = importlib.import_module(task_path)

        task = getattr(task_module, task_name)
        try:
            task(*args, **kwargs)
        except Exception:
            logger.error(
                "Task {} raised error: with args: {} and kwargs: {}: {}".format(
                    full_task_path,
                    args,
                    kwargs,
                    traceback.format_exc(),
                )
            )
        else:
            logger.info(
                "Processing task {} with args: {} and kwargs: {}".format(
                    full_task_path,
                    repr(args),
                    repr(kwargs),
                )
            )


class ManagerWorker(object):

    def __init__(self, queue_prefixes, worker_concurrency):
        self.load_queue_prefixes(queue_prefixes)
        self.queues = self.get_queues_from_queue_prefixes(self.queue_prefixes)
        self.setup_internal_queue(worker_concurrency)
        self.reader_children = []
        self.worker_children = []
        self._initialize_reader_children()
        self._initialize_worker_children(worker_concurrency)

    def _initialize_reader_children(self):
        for queue in self.queues:
            self.reader_children.append(ReadWorker(queue, self.internal_queue))

    def _initialize_worker_children(self, number):
        for index in range(number):
            self.worker_children.append(ProcessWorker(self.internal_queue))

    def load_queue_prefixes(self, queue_prefixes):
        self.queue_prefixes = queue_prefixes

        logger.info("Loading Queues:")
        for queue_prefix in queue_prefixes:
            logger.info("[Queue]\t{}".format(queue_prefix))

    def get_queues_from_queue_prefixes(self, queue_prefixes):
        all_queues = get_conn().get_all_queues()
        matching_queues = []
        for prefix in queue_prefixes:
            matching_queues.extend([
                queue for queue in all_queues if
                fnmatch.fnmatch(queue.name, prefix)
            ])
        logger.info("Found matching SQS Queues: {}".format([q.name for q in matching_queues]))
        return matching_queues

    def setup_internal_queue(self, worker_concurrency):
        self.internal_queue = Queue(worker_concurrency * PREFETCH_MULTIPLIER * MESSAGE_DOWNLOAD_BATCH_SIZE)

    def start(self):
        for child in self.reader_children:
            child.start()
        for child in self.worker_children:
            child.start()

    def stop(self):
        for child in self.reader_children:
            child.shutdown()
        for child in self.reader_children:
            child.join()

        for child in self.worker_children:
            child.shutdown()
        for child in self.worker_children:
            child.join()

    def sleep(self):
        counter = 0
        try:
            while True:
                counter = counter + 1
                if counter % 1000 == 0:
                    counter = 0
                    self.process_counts()
                    self.replace_workers()
                time.sleep(0.001)
        except KeyboardInterrupt:
            logger.debug('Graceful shutdown. Sending shutdown signal to children.')
            self.stop()
            sys.exit(0)

    def process_counts(self):
        reader_count = sum(map(lambda x: x.is_alive(), self.reader_children))
        worker_count = sum(map(lambda x: x.is_alive(), self.worker_children))
        logger.info("Reader Processes: {}".format(reader_count))
        logger.info("Worker Processes: {}".format(worker_count))

    def replace_workers(self):
        self._replace_reader_children()
        self._replace_worker_children()

    def _replace_reader_children(self):
        for index, reader in enumerate(self.reader_children):
            if not reader.is_alive():
                logger.info("Reader Process {} is no longer responding, spawning a new reader.".format(reader.pid))
                queue = reader.sqs_queue
                self.reader_children.pop(index)
                worker = ReadWorker(queue, self.internal_queue)
                worker.start()
                self.reader_children.append(worker)

    def _replace_worker_children(self):
        for index, worker in enumerate(self.worker_children):
            if not worker.is_alive():
                logger.info("Worker Process {} is no longer responding, spawning a new worker.".format(worker.pid))
                self.worker_children.pop(index)
                worker = ProcessWorker(self.internal_queue)
                worker.start()
                self.worker_children.append(worker)
