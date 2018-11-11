import json
import logging

import boto3
from botocore.exceptions import ClientError

from .utils import function_to_import_path

logger = logging.getLogger("pyqs")


def get_or_create_queue(queue_name):
    region_name = boto3.session.Session().region_name
    if not region_name:
        region_name = 'us-east-1'

    sqs = boto3.resource('sqs', region_name=region_name)
    try:
        return sqs.get_queue_by_name(QueueName=queue_name)
    except ClientError as exc:
        non_existent_code = 'AWS.SimpleQueueService.NonExistentQueue'
        if exc.response['Error']['Code'] == non_existent_code:
            return sqs.create_queue(QueueName=queue_name)
        else:
            raise


def task_delayer(func_to_delay, queue_name, delay_seconds=None,
                 override=False):
    function_path = function_to_import_path(func_to_delay, override=override)

    if not queue_name:
        # If no queue specified, use the function_path for the queue
        queue_name = function_path

    def wrapper(*args, **kwargs):
        queue = get_or_create_queue(queue_name)

        _delay_seconds = delay_seconds
        if '_delay_seconds' in kwargs:
            _delay_seconds = kwargs['_delay_seconds']
            del kwargs['_delay_seconds']

        logger.info("Delaying task %s: %s, %s", function_path, args, kwargs)
        message_dict = {
            'task': function_path,
            'args': args,
            'kwargs': kwargs,
        }

        message = json.dumps(message_dict)
        if _delay_seconds is None:
            _delay_seconds = 0
        queue.send_message(MessageBody=message, DelaySeconds=_delay_seconds)

    return wrapper


class task(object):
    def __init__(self, queue=None, delay_seconds=None,
                 custom_function_path=None):
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
        func_to_wrap.delay = task_delayer(
            function, self.queue_name, self.delay_seconds, override=override)
        return func_to_wrap
