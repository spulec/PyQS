import base64
import json
import pickle

import boto3
from datetime import timedelta


def decode_message(message):
    message_body = message['Body']
    json_body = json.loads(message_body)
    if 'task' in message_body:
        return json_body
    else:
        # Fallback to processing celery messages
        return decode_celery_message(json_body['body'])


def decode_celery_message(json_task):
    message = base64.b64decode(json_task)
    try:
        return json.loads(message)
    except ValueError:
        pass
    return pickle.loads(message)


def function_to_import_path(function, override=False):
    if override:
        return function
    return "{}.{}".format(function.__module__, function.__name__)


def get_aws_region_name():
    region_name = boto3.session.Session().region_name
    if not region_name:
        region_name = 'us-east-1'

    return region_name


class TaskContext(object):
    """ Tasks may optionally accept a _context variable. If they do, an
     instance of this object is passed as the context. """

    def __init__(self, conn, queue_url, message_id, receipt_handle):
        self.conn = conn
        self.queue_url = queue_url
        self.message_id = message_id
        self.receipt_handle = receipt_handle

    def change_message_visibility(self, timeout=timedelta(minutes=10)):
        self.conn.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=self.receipt_handle,
            VisibilityTimeout=int(timeout.total_seconds())
        )
