# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import copy
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

from pyqs.utils import get_aws_region_name, decode_message
from pyqs.events import get_events

MESSAGE_DOWNLOAD_BATCH_SIZE = 10
LONG_POLLING_INTERVAL = 20
logger = logging.getLogger("pyqs")


def get_conn(region=None, access_key_id=None, secret_access_key=None):
    if not region:
        region = get_aws_region_name()

    return boto3.client(
        "sqs",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key, region_name=region,
    )


class BaseWorker(Process):
    def __init__(self, *args, **kwargs):
        self._connection = None
        self.parent_id = kwargs.pop('parent_id')
        super(BaseWorker, self).__init__(*args, **kwargs)
        self.should_exit = Event()

    def _get_connection(self):
        if self._connection:
            return self._connection

        if self.connection_args is None:
            self._connection = get_conn()
        else:
            self._connection = get_conn(**self.connection_args)
        return self._connection

    def shutdown(self):
        logger.info(
            "Received shutdown signal, shutting down PID {}!".format(
                os.getpid()))
        self.should_exit.set()

    def parent_is_alive(self):
        if os.getppid() != self.parent_id:
            logger.info(
                "Parent process has gone away, exiting process {}!".format(
                    os.getpid()))
            return False
        return True


class ReadWorker(BaseWorker):

    def __init__(self, queue_url, internal_queue, batchsize,
                 connection_args=None, *args, **kwargs):
        super(ReadWorker, self).__init__(*args, **kwargs)
        if connection_args is None:
            connection_args = {}
        self.connection_args = connection_args
        self.queue_url = queue_url

        sqs_queue = get_conn(**self.connection_args).get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=['All'])['Attributes']
        self.visibility_timeout = int(sqs_queue['VisibilityTimeout'])

        self.internal_queue = internal_queue
        self.batchsize = batchsize

    def run(self):
        # Set the child process to not receive any keyboard interrupts
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        logger.info(
            "Running ReadWorker: {}, pid: {}".format(
                self.queue_url, os.getpid()))
        while not self.should_exit.is_set() and self.parent_is_alive():
            self.read_message()
        self.internal_queue.close()
        self.internal_queue.cancel_join_thread()

    def read_message(self):
        messages = self._get_connection().receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.batchsize,
            WaitTimeSeconds=LONG_POLLING_INTERVAL,
        ).get('Messages', [])

        logger.debug(
            "Successfully got {} messages from SQS queue {}".format(
                len(messages), self.queue_url))  # noqa

        start = time.time()
        for message in messages:
            end = time.time()
            if int(end - start) >= self.visibility_timeout:
                # Don't add any more messages since they have
                # re-appeared in the sqs queue Instead just reset and get
                # fresh messages from the sqs queue
                msg = (
                    "Clearing Local messages since we exceeded "
                    "their visibility_timeout"
                )
                logger.warning(msg)
                break

            message_body = decode_message(message)
            try:
                packed_message = {
                    "queue": self.queue_url,
                    "message": message,
                    "start_time": start,
                    "timeout": self.visibility_timeout,
                }
                self.internal_queue.put(
                    packed_message, True, self.visibility_timeout)
            except Full:
                msg = (
                    "Timed out trying to add the following message "
                    "to the internal queue after {} seconds: {}"
                ).format(self.visibility_timeout, message_body)  # noqa
                logger.warning(msg)
                continue
            else:
                logger.debug(
                    "Message successfully added to internal queue "
                    "from SQS queue {} with body: {}".format(
                        self.queue_url, message_body))  # noqa


class ProcessWorker(BaseWorker):

    def __init__(self, internal_queue, interval, connection_args=None, *args,
                 **kwargs):
        super(ProcessWorker, self).__init__(*args, **kwargs)
        self.connection_args = connection_args
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
        message = packed_message['message']
        queue_url = packed_message['queue']
        fetch_time = packed_message['start_time']
        timeout = packed_message['timeout']
        message_body = decode_message(message)
        full_task_path = message_body['task']
        args = message_body['args']
        kwargs = message_body['kwargs']

        task_name = full_task_path.split(".")[-1]
        task_path = ".".join(full_task_path.split(".")[:-1])

        task_module = importlib.import_module(task_path)

        task = getattr(task_module, task_name)

        pre_process_context = {
            "task_name": task_name,
            "args": args,
            "kwargs": kwargs,
            "full_task_path": full_task_path,
            "fetch_time": fetch_time,
            "queue_url": queue_url,
            "timeout": timeout
        }

        # Modify the contexts separately so the original
        # context isn't modified by later processing
        post_process_context = copy.copy(pre_process_context)

        current_time = time.time()
        if int(current_time - fetch_time) >= timeout:
            logger.warning(
                "Discarding task {} with args: {} and kwargs: {} due to "
                "exceeding visibility timeout".format(  # noqa
                    full_task_path,
                    repr(args),
                    repr(kwargs),
                )
            )
            return True
        try:
            start_time = time.time()
            self._run_hooks("pre_process", pre_process_context)
            task(*args, **kwargs)
        except Exception:
            end_time = time.time()
            logger.exception(
                "Task {} raised error in {:.4f} seconds: with args: {} "
                "and kwargs: {}: {}".format(
                    full_task_path,
                    end_time - start_time,
                    args,
                    kwargs,
                    traceback.format_exc(),
                )
            )
            post_process_context["status"] = "exception"
            post_process_context["exception"] = traceback.format_exc()
            self._run_hooks("post_process", post_process_context)
            return True
        else:
            end_time = time.time()
            self._get_connection().delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            logger.info(
                "Processed task {} in {:.4f} seconds with args: {} "
                "and kwargs: {}".format(
                    full_task_path,
                    end_time - start_time,
                    repr(args),
                    repr(kwargs),
                )
            )
            post_process_context["status"] = "success"
            self._run_hooks("post_process", post_process_context)
        return True

    def _run_hooks(self, hook_name, context):
        hooks = getattr(get_events(), hook_name)
        for hook in hooks:
            hook(context)


class ManagerWorker(object):

    def __init__(self, queue_prefixes, worker_concurrency, interval, batchsize,
                 prefetch_multiplier=2, region=None, access_key_id=None,
                 secret_access_key=None):
        self.connection_args = {
            "region": region,
            "access_key_id": access_key_id,
            "secret_access_key": secret_access_key,
        }
        self.batchsize = batchsize
        if batchsize > MESSAGE_DOWNLOAD_BATCH_SIZE:
            self.batchsize = MESSAGE_DOWNLOAD_BATCH_SIZE
        if batchsize <= 0:
            self.batchsize = 1
        self.interval = interval
        self.prefetch_multiplier = prefetch_multiplier
        self.queue_prefixes = queue_prefixes
        self.queue_urls = self.get_queue_urls_from_queue_prefixes(
            self.queue_prefixes)
        self.setup_internal_queue(worker_concurrency)
        self.reader_children = []
        self.worker_children = []
        self._pid = os.getpid()
        self._initialize_reader_children()
        self._initialize_worker_children(worker_concurrency)
        self._running = True
        self._register_signals()

    def _register_signals(self):
        for SIG in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT,
                    signal.SIGHUP]:
            self.register_shutdown_signal(SIG)

    def _initialize_reader_children(self):
        for queue_url in self.queue_urls:
            self.reader_children.append(
                ReadWorker(
                    queue_url, self.internal_queue, self.batchsize,
                    connection_args=self.connection_args,
                    parent_id=self._pid,
                )
            )

    def _initialize_worker_children(self, number):
        for index in range(number):
            self.worker_children.append(
                ProcessWorker(
                    self.internal_queue, self.interval,
                    connection_args=self.connection_args,
                    parent_id=self._pid,
                )
            )

    def get_queue_urls_from_queue_prefixes(self, queue_prefixes):
        conn = get_conn(**self.connection_args)
        queue_urls = conn.list_queues().get('QueueUrls', [])
        matching_urls = []

        logger.info("Loading Queues:")
        for prefix in queue_prefixes:
            logger.info("[Queue]\t{}".format(prefix))
            matching_urls.extend([
                queue_url for queue_url in queue_urls if
                fnmatch.fnmatch(queue_url.rsplit("/", 1)[1], prefix)
            ])
        logger.info("Found matching SQS Queues: {}".format(matching_urls))
        return matching_urls

    def check_for_new_queues(self):
        queue_urls = self.get_queue_urls_from_queue_prefixes(
            self.queue_prefixes)
        new_queue_urls = set(queue_urls) - set(self.queue_urls)
        for new_queue_url in new_queue_urls:
            logger.info("Found new queue\t{}".format(new_queue_url))
            worker = ReadWorker(
                new_queue_url, self.internal_queue, self.batchsize,
                connection_args=self.connection_args,
                parent_id=self._pid,
            )
            worker.start()
            self.reader_children.append(worker)

    def setup_internal_queue(self, worker_concurrency):
        self.internal_queue = Queue(
            worker_concurrency * self.prefetch_multiplier * self.batchsize)

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
                self.process_counts()
                self.replace_workers()
            if counter % 30000 == 0:
                counter = 0
                self.check_for_new_queues()
            time.sleep(0.001)
        self._exit()

    def register_shutdown_signal(self, SIG):
        signal.signal(SIG, self._graceful_shutdown)

    def _graceful_shutdown(self, signum, frame):
        logger.info('Received shutdown signal %s', signum)
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
                logger.info(
                    "Reader Process {} is no longer responding, "
                    "spawning a new reader.".format(reader.pid))
                queue_url = reader.queue_url
                self.reader_children.pop(index)
                worker = ReadWorker(
                    queue_url, self.internal_queue, self.batchsize,
                    connection_args=self.connection_args,
                    parent_id=self._pid,
                )
                worker.start()
                self.reader_children.append(worker)

    def _replace_worker_children(self):
        for index, worker in enumerate(self.worker_children):
            if not worker.is_alive():
                logger.info(
                    "Worker Process {} is no longer responding, "
                    "spawning a new worker.".format(worker.pid))
                self.worker_children.pop(index)
                worker = ProcessWorker(
                    self.internal_queue, self.interval,
                    connection_args=self.connection_args,
                    parent_id=self._pid,
                )
                worker.start()
                self.worker_children.append(worker)
