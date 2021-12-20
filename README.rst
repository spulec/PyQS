PyQS - Python task-queues for Amazon SQS |Build Status| |Coverage Status|
=========================================================================

**WARNING: This library is still in beta. It has a stable API and has been deployed in production, but we have not received feedback from a large number of use cases, and it is possible there are unknown bugs.**

PyQS is a simple task manager for SQS. It's goal is to provide a simple
and reliable `celery <https://pypi.python.org/pypi/celery>`__-compatible
interface to working with SQS. It uses ``boto3`` under the hood to
`authenticate <https://boto3.readthedocs.io/en/latest/guide/configuration.html>`__
and talk to SQS.

Installation
------------

**PyQS** is available from `PyPI <https://pypi.python.org/>`__ and can
be installed in all the usual ways. To install via *CLI*:

.. code:: bash

    $ pip install pyqs

Or just add it to your ``requirements.txt``.

Usage
-----

PyQS uses some very simple semantics to create and read tasks. Most of
this comes from SQS having a very simple API.

Creating Tasks
~~~~~~~~~~~~~~

Adding a task to queue is pretty simple.

.. code:: python

    from pyqs import task

    @task(queue='email')
    def send_email(subject, message):
        pass

    send_email.delay(subject='Hi there')

**NOTE:** This assumes that you have your AWS keys in the appropriate
environment variables, or are using IAM roles. PyQS doesn't do anything
too special to talk to AWS, it only creates the appropriate ``boto``
connection.

If you don't pass a queue, PyQS will use the function path as the queue
name. For example the following function lives in ``email/tasks.py``.

.. code:: python

    @task()
    def send_email(subject):
        pass

This would show up in the ``email.tasks.send_email`` queue.

You can also specify the function path if you want to reference a function in a different project:

.. code:: python

    @task(custom_function_path="foo.bar.send_email")
    # This references function send_email in foo/bar.py instead of email/tasks.py
    def send_email(subject):
        pass


Reading Tasks
~~~~~~~~~~~~~

To read tasks we need to run PyQS. If the task is already in your
``PYTHON_PATH`` to be imported, we can just run:

.. code:: bash

    $ pyqs email.tasks.send_email

If we want to run all tasks with a certain prefix. This is based on
Python's `fnmatch <http://docs.python.org/2/library/fnmatch.html>`__.

.. code:: bash

    $ pyqs email.*

We can also read from multiple different queues with one call by
delimiting with commas:

.. code:: bash

    $ pyqs send_email,read_email,write_email

If you want to run more workers to process tasks, you can up the
concurrency. This will spawn additional processes to work through
messages.

.. code:: bash

    $ pyqs send_email --concurrency 10

Simple Process Worker
~~~~~~~~~~~~~~~~~~~~~

To use a simpler version of PyQS that deals with some of the edge cases in the original implementation, pass the ``simple-worker`` flag.

.. code:: bash

    $ pyqs send_email --simple-worker

The Simple Process Worker differs in the following way from the original implementation.

* Does not use an internal queue and removes support for the ``prefetch-multiplier`` flag. This helps simply the mental model required, as messages are not on both the SQS queue and an internal queue.
* When the ``simple-worker`` flag is passed, the default ``batchsize`` is 1 instead of 10. This is configurable.
* Does not check the visibility timeout when reading or processing a message from SQS.
    * Allowing the worker to process the message even past its visibility timeout means we solve the problem of never processing a message if ``max_receives=1`` and we incorrectly set a shorter visibility timeout and exceed the visibility timeout. Previously, this message would have ended up in the DLQ, if one was configured, and never actually processed.
    * It increases the probability that we process a message more than once, especially if ``batchsize > 1``, but this can be solved by the developer checking if the message has already been processed.

Hooks
~~~~~

PyQS has an event registry which can be used to run a function before or after every tasks runs.

.. code:: python

    from pyqs import task, events

    def print_pre_process(context):
        print({"pre_process": context})

    def print_post_process(context):
        print({"post_process": context})

    events.register_event("pre_process", print_pre_process)
    events.register_event("post_process", print_post_process)

    @task(queue="my_queue")
    def send_email(subject):
        pass

Operational Notes
~~~~~~~~~~~~~~~~~

**Dead Letter Queues**

It is recommended to use a `Dead Letter Queue <http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html>`__
for any queues that are managed by PyQS.  This is because the current strategy
for fetching messages does not delete them upon initial receipt.  A message is
**ONLY** deleted from SQS upon successful completion. **This is probably
unexpected behavior if you are coming from Celery with SQS.**  Celery attempted
to manage this behavior internally, with varying success.

If an error arises during message processing, it will be discarded and will
re-appear after the visibility timeout.  This can lead to behavior where
there are messages that will never leave the queue and continuously throw
errors.  A Dead Letter Queue helps resolve this by collecting messages that
have be retried a specified number of times.

**Worker Seppuku**

Each process worker will shut itself down after ``100`` tasks have been
processed (or failed to process). This is to prevent issues with stale
connections lingering and blocking tasks forever. In addition it helps
guard against memory leaks, though in a rather brutish fashion. After
the process worker shut itself down the managing process should notice
and restart it promptly. The value of ``100`` is currently hard-coded,
but could be configurable.

**Queue Blocking**

While there are multiple workers for reading from different queues, they
all append to the same internal queue. This means that if you have one
queue with lots of fast tasks, and another with a few slow tasks, they
can block eachother and the fast tasks can build up behind the slow
tasks. The simplest solution is to just run two different ``PyQS``
commands, one for each queue with appropriate concurrency settings.

**Visibility Timeout**

Care is taken to not process messages that have exceeded the visibility
timeout of their queue. The goal is to prevent double processing of
tasks. However, it is still quite possible for this to happen since we
do not use transactional semantics around tasks. Therefore, it is
important to properly set the visibility timeout on your queues based on
the expected length of your tasks. If the timeout is too short, tasks
will be processed twice, or very slowly. If it is too long, ephemeral
failures will delay messages and reduce the queue throughput
drastically. This is related to the queue blocking described above as
well. SQS queues are free, so it is good practice to keep the messages
stored in each as homogenous as possible.

Compatibility
~~~~~~~~~~~~~

**Celery:**

PyQS was created to replace celery inside of our infrastructure. To
achieve this goal we wanted to make sure we were compatible with the
basic Celery APIs. To this end, you can easily start trying out PyQS in
your Celery-based system. PyQS can read messages that Celery has written
to SQS. It will read ``pickle`` and ``json`` serialized SQS messages
(Although we recommend JSON).

**Operating Systems:**

UNIX. Due to the use of the ``os.getppid`` system call. This feature can
probably be worked around if anyone actually wants windows support.

**Boto3:**

Currently PyQS only supports a few basic connection parameters being
explicitly passed to the connection. Any work ``boto3`` does to
transparently find connection credentials, such as IAM roles, will still
work properly.

When running PyQS from the command-line you can pass ``--region``,
``--access-key-id``, and ``--secret-access-key`` to override the default
values.

Caveats
~~~~~~~

**Durability:**

When we read a batch of messages from SQS we attempt to add them to our
internal queue until we exceed the visibility timeout of the queue. Once
this is exceeded, we discard the messages and grab a new batch.
Additionally, when a process worker gets a message from the internal
queue, the time the message was fetched from SQS is checked against the
queues visibility timeout and discarded if it exceeds the timeout. The
goal is to reduce double processing. However, this system does not
provide transactions and there are cases where it is possible to process
a message whos' visibility timeout has been exceeded. It is up to you to
make sure that you can handle this edge case.

**Task Importing:**

Currently there is not advanced logic in place to find the location of
modules to import tasks for processing. PyQS will try using
``importlib`` to get the module, and then find the task inside the
module. Currently we wrap our usage of PyQS inside a Django admin
command, which simplifies task importing. We call the
`\*\*\_main()\*\* <https://github.com/spulec/PyQS/blob/master/pyqs/main.py#L53>`__
method directly, skipping **main()** since it only performs argument
parsing.

**Running inside of containers**

PyQS assumes that the process id is not 1. If you are running PyQS inside of a
container, you should wrap it in supervisor or something like `dummy-init <https://github.com/Yelp/dumb-init>`__.

**Why not just use Celery?**

We like Celery. We `(Yipit.com) <http://yipit.com/about/team/>`__ even
sponsored the `original SQS
implementation <https://github.com/celery/kombu/commit/1ab629c23c85aeabf5a4c9a6bb570e8da822c3a6>`__.
However, SQS is pretty different from the rest of the backends that
Celery supports. Additionally the Celery team does not have the
resources to create a robust SQS implementation in addition to the rest
of their duties. This means the SQS is carrying around a lot extra
features and a complex codebase that makes it hard to debug.

We have personally experienced some very vexing resource leaks with
Celery that have been hard to trackdown. For our use case, it has been
simpler to switch to a simple library that we fully understand. As this
library evolves that may change and the the costs of switching may not
be worth it. However, we want to provide the option to others who use
python and SQS to use a simpler setup.

.. |Build Status| image:: https://travis-ci.org/spulec/PyQS.svg?branch=master
   :target: https://travis-ci.org/spulec/PyQS
.. |Coverage Status| image:: https://coveralls.io/repos/spulec/PyQS/badge.svg?branch=master&service=github
   :target: https://coveralls.io/github/spulec/PyQS?branch=master
