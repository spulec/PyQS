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
try:
    from queue import Empty, Full
except ImportError:
    from Queue import Empty, Full

import boto3

from pyqs.utils import decode_message

MESSAGE_DOWNLOAD_BATCH_SIZE = 10
LONG_POLLING_INTERVAL = 20
logger = logging.getLogger("pyqs")


class BaseWorker(Process):
    def __init__(self, *args, **kwargs):
        super(BaseWorker, self).__init__(*args, **kwargs)
        self.should_exit = Event()

    def shutdown(self):
        logger.info("Received shutdown signal, shutting down PID {}!".format(os.getpid()))
        self.should_exit.set()

    def parent_is_alive(self):
        if os.getppid() == 1:
            logger.info("Parent process has gone away, exiting process {}!".format(os.getpid()))
            return False
        return True


class ReadWorker(BaseWorker):

    def __init__(self, sqs_queue, internal_queue, batchsize, *args, **kwargs):
        super(ReadWorker, self).__init__(*args, **kwargs)
        self.sqs_queue = sqs_queue
        self.visibility_timeout = float(self.sqs_queue.attributes['VisibilityTimeout'])
        self.internal_queue = internal_queue
        self.batchsize = batchsize

    def run(self):
        # Set the child process to not receive any keyboard interrupts
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        logger.info("Running ReadWorker: {}, pid: {}".format(self.sqs_queue.url, os.getpid()))
        while not self.should_exit.is_set() and self.parent_is_alive():
            self.read_message()
        self.internal_queue.close()
        self.internal_queue.cancel_join_thread()

    def read_message(self):
        messages = self.sqs_queue.receive_messages(MaxNumberOfMessages=self.batchsize, WaitTimeSeconds=LONG_POLLING_INTERVAL)
        logger.info("Successfully got {} messages from SQS queue {}".format(len(messages), self.sqs_queue.url))  # noqa
        start = time.time()
        for message in messages:
            logger.debug("Message loop")
            end = time.time()
            if int(end - start) >= self.visibility_timeout:
                # Don't add any more messages since they have re-appeared in the sqs queue
                # Instead just reset and get fresh messages from the sqs queue
                msg = "Clearing Local messages since we exceeded their visibility_timeout"
                logger.warning(msg)
                break

            message_body = decode_message(message)
            try:
                packed_message = {
                    "queue_url": self.sqs_queue.url,
                    "message": message_body,
                    "receipt_handle": message.receipt_handle,
                    "start_time": start,
                    "timeout": self.visibility_timeout,
                }
                self.internal_queue.put(packed_message, True, self.visibility_timeout)
            except Full:
                msg = "Timed out trying to add the following message to the internal queue after {} seconds: {}".format(self.visibility_timeout, message_body)  # noqa
                logger.warning(msg)
                continue
            else:
                logger.debug("Message successfully added to internal queue from SQS queue {} with body: {}".format(self.sqs_queue.url, message_body))  # noqa


class ProcessWorker(BaseWorker):

    def __init__(self, internal_queue, interval, *args, **kwargs):
        super(ProcessWorker, self).__init__(*args, **kwargs)
        self.conn = boto3.client('sqs')
        self.internal_queue = internal_queue
        self.interval = interval
        self._messages_to_process_before_shutdown = 100

    def run(self):
        # Set the child process to not receive any keyboard interrupts
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        logger.info("Running ProcessWorker, pid: {}".format(os.getpid()))
        messages_processed = 0
        while not self.should_exit.is_set() and self.parent_is_alive():
            processed = self.process_message()
            if processed:
                messages_processed += 1
                time.sleep(self.interval)
            else:
                # If we have no messages wait a moment before rechecking.
                time.sleep(0.001)
            if messages_processed >= self._messages_to_process_before_shutdown:
                self.shutdown()

    def process_message(self):
        try:
            packed_message = self.internal_queue.get(timeout=0.5)
        except Empty:
            # Return False if we did not attempt to process any messages
            return False
        message_body = packed_message['message']
        queue_url = packed_message['queue_url']
        fetch_time = packed_message['start_time']
        timeout = packed_message['timeout']
        receipt_handle = packed_message['receipt_handle']
        full_task_path = message_body['task']
        args = message_body['args']
        kwargs = message_body['kwargs']

        task_name = full_task_path.split(".")[-1]
        task_path = ".".join(full_task_path.split(".")[:-1])

        task_module = importlib.import_module(task_path)

        task = getattr(task_module, task_name)

        current_time = time.time()
        if int(current_time - fetch_time) >= timeout:
            logger.warning(
                "Discarding task {} with args: {} and kwargs: {} due to exceeding visibility timeout".format(  # noqa
                    full_task_path,
                    repr(args),
                    repr(kwargs),
                )
            )
            return True
        try:
            start_time = time.clock()
            task(*args, **kwargs)
        except Exception:
            end_time = time.clock()
            logger.exception(
                "Task {} raised error in {:.4f} seconds: with args: {} and kwargs: {}: {}".format(
                    full_task_path,
                    end_time - start_time,
                    args,
                    kwargs,
                    traceback.format_exc(),
                )
            )
            return True
        else:
            end_time = time.clock()

            boto3.resource('sqs').Message(queue_url, receipt_handle).delete()
            logger.info(
                "Processed task {} in {:.4f} seconds with args: {} and kwargs: {}".format(
                    full_task_path,
                    end_time - start_time,
                    repr(args),
                    repr(kwargs),
                )
            )
        return True


class ManagerWorker(object):

    def __init__(self, queue_prefixes, worker_concurrency, interval, batchsize, prefetch_multiplier=2):
        self.batchsize = batchsize
        if batchsize > MESSAGE_DOWNLOAD_BATCH_SIZE:
            self.batchsize = MESSAGE_DOWNLOAD_BATCH_SIZE
        if batchsize <= 0:
            self.batchsize = 1
        self.interval = interval
        self.prefetch_multiplier = prefetch_multiplier
        self.load_queue_prefixes(queue_prefixes)
        self.queues = self.get_queues_from_queue_prefixes(self.queue_prefixes)
        self.setup_internal_queue(worker_concurrency)
        self.reader_children = []
        self.worker_children = []
        self._initialize_reader_children()
        self._initialize_worker_children(worker_concurrency)
        self._running = True
        self._register_signals()

    def _register_signals(self):
        for SIG in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGHUP]:
            self.register_shutdown_signal(SIG)

    def _initialize_reader_children(self):
        for queue in self.queues:
            self.reader_children.append(ReadWorker(queue, self.internal_queue, self.batchsize))

    def _initialize_worker_children(self, number):
        for index in range(number):
            self.worker_children.append(ProcessWorker(self.internal_queue, self.interval))

    def load_queue_prefixes(self, queue_prefixes):
        self.queue_prefixes = queue_prefixes

        logger.info("Loading Queues:")
        for queue_prefix in queue_prefixes:
            logger.info("[Queue]\t{}".format(queue_prefix))

    def get_queues_from_queue_prefixes(self, queue_prefixes):
        queue_urls = boto3.client('sqs').list_queues()['QueueUrls']

        logger.debug("Queue_urls: %s", queue_urls)

        matching_queues = []
        for prefix in queue_prefixes:
            # replace dots with underscores, so we match the queue creation
            prefix = prefix.replace('.', '_')
            for queue_url in queue_urls:
                if fnmatch.fnmatch(queue_url.split('/')[-1], prefix):
                    matching_queues.append(boto3.resource('sqs').Queue(queue_url))

        logger.info("Found matching SQS Queues: {}".format([q.url for q in matching_queues]))
        return matching_queues

    def setup_internal_queue(self, worker_concurrency):
        self.internal_queue = Queue(worker_concurrency * self.prefetch_multiplier * self.batchsize)

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
        while self._running:
            counter = counter + 1
            if counter % 1000 == 0:
                counter = 0
                self.process_counts()
                self.replace_workers()
            time.sleep(0.001)
        self._exit()

    def register_shutdown_signal(self, SIG):
        signal.signal(SIG, self._graceful_shutdown)

    def _graceful_shutdown(self, signum, frame):
        self._running = False

    def _exit(self):
        logger.info('Graceful shutdown. Sending shutdown signal to children.')
        self.stop()
        sys.exit(0)

    def process_counts(self):
        reader_count = sum(map(lambda x: x.is_alive(), self.reader_children))
        worker_count = sum(map(lambda x: x.is_alive(), self.worker_children))
        logger.debug("Reader Processes: {}".format(reader_count))
        logger.debug("Worker Processes: {}".format(worker_count))

    def replace_workers(self):
        self._replace_reader_children()
        self._replace_worker_children()

    def _replace_reader_children(self):
        for index, reader in enumerate(self.reader_children):
            if not reader.is_alive():
                logger.info("Reader Process {} is no longer responding, spawning a new reader.".format(reader.pid))
                queue = reader.sqs_queue
                self.reader_children.pop(index)
                worker = ReadWorker(queue, self.internal_queue, self.batchsize)
                worker.start()
                self.reader_children.append(worker)

    def _replace_worker_children(self):
        for index, worker in enumerate(self.worker_children):
            if not worker.is_alive():
                logger.info("Worker Process {} is no longer responding, spawning a new worker.".format(worker.pid))
                self.worker_children.pop(index)
                worker = ProcessWorker(self.internal_queue, self.interval)
                worker.start()
                self.worker_children.append(worker)
