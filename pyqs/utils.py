import base64
import json
import pickle


def decode_message(message):
    message_body = message.get_body()
    json_body = json.loads(message_body)
    if 'task' in message_body:
        return json_body
    else:
        # Fallback to processing celery messages
        return decode_celery_message(json_body)


def decode_celery_message(json_task):
    message = base64.b64decode(json_task['body'])
    try:
        return json.loads(message)
    except ValueError:
        pass
    return pickle.loads(message)


def function_to_import_path(function, override=False):
    if override:
        return function
    return "{}.{}".format(function.__module__, function.__name__)
