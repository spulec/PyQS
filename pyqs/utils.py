import base64
import json
import pickle


def decode_message(message):
    message_body = message.get_body()
    try:
        return json.loads(message_body)
    except ValueError:
        # Fallback to processing celery messages
        return decode_celery_message(message_body)


def decode_celery_message(text):
    task = base64.decodestring(text + "==")
    json_task = json.loads(task)
    message = base64.decodestring(json_task['body'])
    return pickle.loads(message)
