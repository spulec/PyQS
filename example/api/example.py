import logging
import json
from flask import Blueprint, current_app, request, jsonify
from api.helpers import construct_response
from api import tasks

blueprint = Blueprint('api', __name__)


@blueprint.route('/example', methods=['POST'])
def example():
    queues = current_app.config.get('QUEUES')
    queue_name = queues['example']['name']

    payload = {}
    data = request.get_data()

    try:
        message_data = json.loads(data)
    except (TypeError, AttributeError, ValueError) as e:
        status = 500
        return construct_response(
            'Your payload does not appear to be valid json!', data, status)

    try:
        tasks.process.delay(message=message_data)
        response_message = (
            f'Successfully submitted message to queue {queue_name}'
        )
        status = 200
    except Exception as e:
        response_message = (
            f'Something went wrong submitting message '
            'to queue {queue_name}! {e}'
        )
        status = 500

    return construct_response(response_message, message_data, status)


@blueprint.route('/health', methods=['GET'])
def health():
    return jsonify('OK'), 200


@blueprint.route('/', methods=['GET'])
def hello():
    return 'Hello!'
