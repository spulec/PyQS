import boto
import importlib
import json


def main(queue_name):
    conn = boto.connect_sqs()
    queue = conn.get_queue(queue_name)

    message = queue.read()
    message_body = json.loads(message.get_body())

    task_path = message_body['task']
    args = message_body['args']
    kwargs = message_body['kwargs']

    task_name = task_path.split(".")[-1]
    task_path = ".".join(task_path.split(".")[:-1])

    task_module = importlib.import_module(task_path)

    task = getattr(task_module, task_name)

    task(*args, **kwargs)
