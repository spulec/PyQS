# PyQs example

This example Flask app will:

1. Accept a JSON payload at `/example`
2. Publish the message to SQS with attributes that allow PyQS to process it (see [example.py](api/example.py))
3. PyQS workers listening to the queue will process the messages with the specified task name

Make sure you plug in your AWS API credentials to [env](env) and that a SQS queue called `queue-example` is created.

## Running with docker:

* Clone this repo and cd to the root
* Build image: `docker build -t pyqs-example .`
* Run container: `docker run --rm --env-file env -it pyqs-example`
* Post data: `docker exec -it $(docker ps | grep "pyqs-example" | awk '{print $1}') /bin/bash  -c "curl -X POST -H 'Content-Type: application/json' -d '{\"message\": \"Testing\"}' http://localhost:8000/example"`

## Specifying PyQS task to be used to process your message

A PyQS worker listening to a queue with messages that specify a `task` path in them will be routed appropriately for processing. Basically, your message payload to SQS ends up looking something like this:

```
{
  "body": {"some": "data"},
  "task": "api.tasks.process"
  "args": [],
  "kwargs": { "body": {"some": "data"} }
}
```

In the above data sample, the message will be routed to `process()` in `api/tasks.py`. In the task function, you can do whatever is required (email, slack notify, push to new queue, etc). See [example.py](api/example.py) and [tasks](api/tasks.py) for more information.  
