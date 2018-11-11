import logging
from flask import Flask, current_app
from pyqs import task


# This task listens to 'queue-example' SQS queues for messages with
# {"task": "process"}
@task(queue='queue-example')
def process(message):
    logging.info(
        f'PyQS task process() is processing message {message}. '
        'Clap your hands!'
    )
