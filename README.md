## PyQS - Python task-queues for Amazon SQS [![Build Status](https://travis-ci.org/spulec/PyQS.svg?branch=master)](https://travis-ci.org/spulec/PyQS) [![Coverage Status](https://coveralls.io/repos/spulec/PyQS/badge.svg?branch=master&service=github)](https://coveralls.io/github/spulec/PyQS?branch=master)

**WARNING: This library is still in beta. It can do anything up to and including eating your laundry.**

PyQS is a simple task manager for SQS.  It's goal is to provide a simple and reliable [celery](https://pypi.python.org/pypi/celery)-compatible interface to working with SQS.  It uses `boto` under the hood to [authenticate](https://boto.readthedocs.org/en/latest/boto_config_tut.html) and talk to SQS.

### Installation

**PyQS** is available from [PyPI](https://pypi.python.org/) and can be installed in all the usual ways.  To install via *CLI*:

```bash
$ pip install pyqs
```

Or just add it to your `requirements.txt`.

### Usage

PyQS uses some very simple semantics to create and read tasks.  Most of this comes from SQS having a very simple API.

#### Creating Tasks

Adding a task to queue is pretty simple.

```python
from pyqs import task

@task(queue='email')
def send_email(subject, message):
    pass

send_email.delay(subject='Hi there')
```
**NOTE:** This assumes that you have your AWS keys in the appropriate environment variables, or are using IAM roles. PyQS doesn't do anything too special to talk to AWS, it only creates the appropriate `boto` connection.

If you don't pass a queue, PyQS will use the function path as the queue name. For example the following function lives in `email/tasks.py`.

```python
@task()
def send_email(subject):
    pass
```

This would show up in the `email.tasks.send_email` queue.


#### Reading Tasks

To read tasks we need to run PyQS.  If the task is already in your `PYTHON_PATH` to be imported, we can just run:

```bash
$ pyqs email.tasks.send_email
```

If we want want to run all tasks with a certain prefix. This is based on Python's [fnmatch](http://docs.python.org/2/library/fnmatch.html).

```bash
$ pyqs email.*
```

We can also read from multiple different queues with one call by delimiting with commas:

```bash
$ pyqs send_email,read_email,write_email
```

If you want to run more workers to process tasks, you can up the concurrency.  This will spawn additional processes to work through messages.

```bash
$ pyqs send_email --concurrency 10
```

#### Compatability

**Celery:**

PyQS was created to replace celery inside of our infrastructure.  To achieve this goal we wanted to make sure we were compatible with the basic Celery APIs.  To this end, you can easily start trying out PyQS in your Celery-based system.  PyQS can read messages that Celery has written to SQS. It will read `pickle` and `json` serialized SQS messages (Although we recommend JSON).

**Operating Systems:**

UNIX.  Due to the use of the `os.getppid` system call.  This feature can probably be worked around if anyone actually wants windows support.

**Boto:**

Currently PyQS only supports a few basic connection parameters being explicitly passed to the connection. Any work `boto` does to transparently find connection credentials, such as IAM roles, will still work properly.

When running PyQS from the command-line you can pass `--region`, `--access-key-id`, and `--secret-access-key` to override the default values.

#### Caveats

**Durability:**

Right now, once we have pulled down a message from SQS and successfully added it to our internal queue, we delete it from SQS.  This means it is possible to lose messages that have been added to the internal queue but were not processed before shutdown / crashing.

When we read a batch of messages from SQS we attempt to add them to our internal queue until we exceed the visibility timeout of the queue.  Once this is exceeded, we discard the messages and grab a new batch.  The goal is to reduce double processing.  However, this system does not provide transactions and there are cases where it is possible to process a message who's visibility timeout has been exceeded.  It is up to you to make sure that you can handle this edge case.

**Task Importing:**

Currently there is not advanced logic in place to find the location of modules to import tasks for processing.  PyQS will try using `importlib` to get the module, and then find the task inside the module.  Currently we wrap our usage of PyQS inside a Django admin command, which simplifies task importing.  We call the [**_main()**](https://github.com/spulec/PyQS/blob/master/pyqs/main.py#L53) method directly, skipping **main()** since it only performs argument parsing.


**Why not just use Celery?**

We like Celery.  We [(Yipit.com)](http://yipit.com/about/team/) even sponsored the [original SQS implementation](https://github.com/celery/kombu/commit/1ab629c23c85aeabf5a4c9a6bb570e8da822c3a6). However, SQS is pretty different from the rest of the backends that Celery supports.  Additionally the Celery team does not have the resources to create a robust SQS implementation in addition to the rest of their duties.  This means the SQS is carrying around a lot extra features and a complex codebase that makes it hard to debug.

We have personally experienced some very vexing resource leaks with Celery that have been hard to trackdown.  For our use case, it has been simpler to switch to a simple library that we fully understand.  As this library evolves that may change and the the costs of switching may not be worth it.  However, we want to provide the option to others who use python and SQS to use a simpler setup.
