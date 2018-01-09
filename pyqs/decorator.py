import json
import logging

import boto3
from botocore.exceptions import ClientError

from .utils import function_to_import_path

logger = logging.getLogger("pyqs")


def get_or_create_queue(queue_name):
    sqs = boto3.resource('sqs')
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
    except ClientError as e:
        if 'QueueDoesNotExist' == e.__class__.__name__:
            queue = sqs.create_queue(QueueName=queue_name)
        elif 'ClientError' == e.__class__.__name__:
            # moto client :(
            queue = sqs.create_queue(QueueName=queue_name)
        else:
            raise(e)
    return queue


def task_delayer(func_to_delay, queue_name, delay_seconds=None, override=False):
    function_path = function_to_import_path(func_to_delay, override=override)

    if not queue_name:
        # If no queue specified, use the function_path for the queue, be sure
        # to replace the dots with underscores, as dots are not accepted as
        # queue names :(
        queue_name = function_path.replace('.', '-')

    def wrapper(*args, **kwargs):
        queue = get_or_create_queue(queue_name)

        _delay_seconds = delay_seconds
        if _delay_seconds is None:
            _delay_seconds = 0
        if '_delay_seconds' in kwargs:
            _delay_seconds = kwargs['_delay_seconds']
            del kwargs['_delay_seconds']

        logger.info("Delaying task %s: %s, %s", function_path, args, kwargs)
        message_dict = {
            'task': function_path,
            'args': args,
            'kwargs': kwargs,
        }

        queue.send_message(
            MessageBody=json.dumps(message_dict),
            DelaySeconds=_delay_seconds,
        )

    return wrapper


class task(object):
    def __init__(self, queue=None, delay_seconds=None, custom_function_path=None):
        self.queue_name = queue
        self.delay_seconds = delay_seconds
        self.function_path = custom_function_path

    def __call__(self, *args, **kwargs):
        func_to_wrap = args[0]
        function = func_to_wrap
        override = False
        if self.function_path:
            override = True
            function = self.function_path
        func_to_wrap.delay = task_delayer(function, self.queue_name, self.delay_seconds, override=override)
        return func_to_wrap
