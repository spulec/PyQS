import json
import logging
import boto3
from flask import jsonify

class SqsClient(object):
    def __init__(self, queue):
        self.queue = queue

    def send(self, data):
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.queue)

        try:
            message_string = json.dumps(data)
        except (TypeError, AttributeError) as e:
            logging.exception(f'{repr(data)} - {e}')

        response = queue.send_message(
            MessageBody=message_string,
        )

        message_id = response['MessageId']
        logging.info(f'Submitted message {message_id} to queue {self.queue} successfully')

        return message_id

def construct_response(message, payload, status):
    body = {}

    if status == 500:
        body['message'] = 'Something went wrong constructing response. Is your payload valid JSON?'
        body['request_payload'] = str(payload)
    else:
        body['message'] = message
        body['request_payload'] = payload

    body['status_code'] = status
    logging.debug(body)

    resp = jsonify(body)
    resp.status_code = status

    return resp
