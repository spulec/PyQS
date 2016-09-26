import json
import logging
import os
import boto
from boto.sqs.message import Message
from .utils import function_to_import_path, get_conn
from functools import lru_cache

PYQS_GET_QUEUE_CACHE_MAX_SIZE = \
    int(os.environ.get('PYQS_GET_QUEUE_CACHE_MAX_SIZE', 32))


logger = logging.getLogger("pyqs")

conn = None


def do_get_or_create_queue(queue_name):
    global conn
    if conn is None:
        conn = get_conn()
    queue = conn.get_queue(queue_name)
    if queue:
        return queue
    else:
        return conn.create_queue(queue_name)


@lru_cache(maxsize=PYQS_GET_QUEUE_CACHE_MAX_SIZE)
def cached_get_or_create_queue(queue_name):
    return do_get_or_create_queue(queue_name)


def get_or_create_queue(queue_name):
    if bool(os.environ.get('PYQS_GET_QUEUE_CACHE', False)):
        return cached_get_or_create_queue(queue_name)
    else:
        return do_get_or_create_queue(queue_name)


def task_delayer(func_to_delay, queue_name):
    function_path = function_to_import_path(func_to_delay)

    if not queue_name:
        # If no queue specified, use the function_path for the queue
        queue_name = function_path

    def wrapper(*args, **kwargs):
        queue = get_or_create_queue(queue_name)
        logger.info("Delaying task %s: %s, %s", function_path, args, kwargs)
        message_dict = {
            'task': function_path,
            'args': args,
            'kwargs': kwargs,
        }

        message = Message()
        message.set_body(json.dumps(message_dict))
        queue.write(message)

    return wrapper


class task(object):
    def __init__(self, queue=None):
        self.queue_name = queue

    def __call__(self, *args, **kwargs):
        func_to_wrap = args[0]
        func_to_wrap.delay = task_delayer(func_to_wrap, self.queue_name)
        return func_to_wrap
