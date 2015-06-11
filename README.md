# PyQS - Python task-queues for Amazon SQS

[![Build Status](https://travis-ci.org/spulec/PyQS.png?branch=master)](https://travis-ci.org/spulec/PyQS)

# WARNING: This library is still under active development and should not be used in production.

# In a nutshell

PyQS is a simple task-queue for SQS.

email/tasks.py
```python
@task()
def send_email(subject):
    pass
```

Then later run
```python
send_email.delay(subject='Hi there')
```

This task will get enqueued to a new 'emailer.tasks.send_email' queue.

If you wanted to put it on a particular queue, say 'email', you can do

email/tasks.py
```python
@task(queue='email')
def send_email(subject):
    pass
```


Run the worker:

```bash
$ pyqs email.tasks.send_email
```

Or to run all tasks

```bash
$ pyqs email.*
```

This is based on Python's [fnmatch](http://docs.python.org/2/library/fnmatch.html).


## Exception Handling
- 'failure' queue?
- special logger?
- sentry integration?
