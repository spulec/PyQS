import json
import logging
from flask import jsonify


def construct_response(message, payload, status):
    body = {}

    if status == 500:
        body['message'] = (
            'Something went wrong constructing response. '
            'Is your payload valid JSON?'
        )
        body['request_payload'] = str(payload)
    else:
        body['message'] = message
        body['request_payload'] = payload

    body['status_code'] = status
    logging.debug(body)

    resp = jsonify(body)
    resp.status_code = status

    return resp
