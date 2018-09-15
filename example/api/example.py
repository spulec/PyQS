import logging
import json
from flask import Blueprint, current_app, request, jsonify
from api.helpers import SqsClient
from api.helpers import construct_response

blueprint = Blueprint('api', __name__)

@blueprint.route('/example', methods=['POST'])
def example():
    queues = current_app.config.get('QUEUES')
    queue_name = queues['example']['name']

    payload = {}
    data = request.get_data()

    try:
        message_data = json.loads(data)
    except (TypeError, AttributeError) as e:
        status = 500
        return construct_response('', data, status)

    sqs = SqsClient(queue_name)
    payload['body'] = message_data # PyQS wants this key to exist
    payload['task'] = 'api.tasks.process' # Module path of PyQS task to process with
    payload['args'] = [] # Normal args that can be passed to task
    payload['kwargs'] = {'message': message_data} # To pass kwargs to task - necessary?

    message_id = sqs.send(payload)
    message = f"Sucessfully recieved request and submitted to queue {queue_name } with id {message_id}"
    logging.info(message)

    if message_id:
        status = 200
    else:
        status = 500

    return construct_response(message, message_data, status)


@blueprint.route('/health', methods=['GET'])
def health():
    return jsonify('OK'), 200

@blueprint.route('/', methods=['GET'])
def hello():
    return 'Hello!'
