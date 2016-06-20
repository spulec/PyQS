import base64
import json
import pickle
import os
import boto
import boto.sqs


def decode_message(message):
    message_body = message.get_body()
    json_body = json.loads(message_body)
    if 'task' in message_body:
        return json_body
    else:
        # Fallback to processing celery messages
        return decode_celery_message(json_body)


def decode_celery_message(json_task):
    message = base64.decodestring(json_task['body'])
    try:
        return json.loads(message)
    except ValueError:
        pass
    return pickle.loads(message)


def function_to_import_path(function):
    return "{}.{}".format(function.__module__, function.__name__)


def get_conn(region=None, access_key_id=None, secret_access_key=None,
             endpoint_url=None):
    if endpoint_url:
        host, port = endpoint_url.split(':')
        local_region = boto.sqs.regioninfo.RegionInfo(name='elasticmq',
                                                      endpoint=host)
        return boto.connect_sqs(aws_access_key_id='x',
                                aws_secret_access_key='x',
                                is_secure=False,
                                port=port,
                                region=local_region)
    elif access_key_id is not None and secret_access_key is not None:
        return boto.connect_sqs(aws_access_key_id=access_key_id,
                                aws_secret_access_key=secret_access_key,
                                region=_get_region(region))
    else:
        return boto.connect_sqs(
                    region=_get_region(
                            os.environ.get('AWS_DEFAULT_REGION', None)))


def _get_region(region_name):
    if region_name is not None:
        for region in boto.sqs.regions():
            if region.name == region_name:
                return region
