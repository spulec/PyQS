## PyQS - Python task-queues for Amazon SQS [![Build Status](https://travis-ci.org/spulec/PyQS.svg?branch=master)](https://travis-ci.org/spulec/PyQS)

**WARNING: This library is still in beta. It can do anything up to and including eating your laundry.**

PyQS is a simple task manager for SQS.  It's goal is to provide a simple and reliable celery-compatible interface to working with SQS.


### Installation

**PyQS** is available from [PyPI](https://pypi.python.org/) and can be installed in all the usual ways.  To install via *CLI*:

```
pip install pyqs
```

Or just add it to your `requirements.txt`.

### Usage

#### Creating tasks

```python
from pyqs import task

@task(queue='email')
def send_email(subject, message):
    pass
```

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

#### Compatability

UNIX.  Due to the use of the `os.getppid` system call.  This feature can probably be worked around if anyone actually wants windows support.
