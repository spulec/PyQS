# PyQs example

This example Flask app will:

1. Accept a JSON payload at `/example`
2. Publish the message (see [example.py](api/example.py))
3. PyQS workers listening to the queue will process the messages with the specified task name

Make sure you plug in your AWS API credentials to [env](env) and that a SQS queue called `queue-example` is created.

## Running with docker:

* Clone this repo and cd to the root
* Build image: `docker build -t pyqs-example .`
* Run container: `docker run --rm --env-file env -it pyqs-example`
* Post data: `docker exec -it $(docker ps | grep "pyqs-example" | awk '{print $1}') /bin/bash  -c "curl -X POST -H 'Content-Type: application/json' -d '{\"message\": \"Testing\"}' http://localhost:8000/example"`

## Specifying PyQS task to be used to process your message

Submitting a message to a queue is detailed in the main readme. In this example, `api/tasks.py` has a `process()` tasks that is used to process messages submitted using `tasks.process.delay(message=message_data)` in [example.py](api/example.py). See [example.py](api/example.py) and [tasks](api/tasks.py) for more information.
