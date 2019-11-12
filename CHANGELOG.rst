Changelog
---------

0.1.4
~~~~~

- Improve behavior when a queue is created after a worker has started. The worker will now refresh the queues every 30 seconds to check for new queues.

0.1.3
~~~~~

- Change PPID checking to check for actual parent ID, instead of `PID 1`.  This fixes issues running on docker containers where PPID of 1 is expected.

0.1.2
~~~~~

- 419ce2e Merge pull request #56 from orangain/honor-aws-region
- 7c793d0 Merge pull request #55 from orangain/fix-indentation-error
- 0643fbb Honor aws region configured by .aws/config or env var
- f5c1db9 Fix indentation error
- cdae257 Merge pull request #52 from cedarai/master
- a2ac378 Merge pull request #53 from p1c2u/fix/nosetest-remove-stop-parameter
- dbaa391 Merge pull request #51 from p1c2u/fix/pep8-styles-fixes
- 1577382 Nosetest remove stop parameter
- b7420e3 Add current directory to PYTHONPATH
- 8d04b62 Graceful shutdown logging msg fix
- 796acbc PEP8 styles fixes
- 72dcb62 Merge pull request #50 from hobbsh/add_example
- d00d31f Update readme
- dfbf459 Use .delay() to submit messages
- 612158f Merge pull request #49 from hobbsh/no_log_0_msg
- 09a649f Use logger.debug for success SQS log line
- dfd56c3 Fix typos in readme
- a774155 Add example flask app
- 17e7b7c Don't log message retrieve success when there are 0 messages
- 14eb827 Add shutdown signal logging.

0.1.1
~~~~~

- Fix KeyError on accounts without queues

0.1.0
~~~~~

- Upgrade to boto3

0.0.22
~~~~~~

- Fix Python 3 support
- Allow overwriting the `delay_seconds` attibute at call time

0.0.21
~~~~~~

- Add ability to tune ``PREFETCH_MULTIPLIER`` with ``--prefetch-multiplier``.

0.0.20
~~~~~~

- Respect ``--batch-size`` when sizing internal queue on ManagerWorker

0.0.19
~~~~~~

- Add ability to run with tunable BATCHSIZE and INTERVAL. Thanks to @masayang
- Add ability to create tasks with a visibility delay.  Thanks to @joshbuddy
- Add ability to create tasks with a custom function location, allowing cross project tasks

0.0.18
~~~~~~

- Convert Changelog to .rst
- Add Changelog to long description on Pypi.  Thanks to @adamchainz

0.0.17
~~~~~~

-  Fix typos in README
-  Add notes on Dead Letter Queues to README

0.0.16
~~~~~~

-  Switch README to reStructuredText (.rst) format so it renders on PyPI

0.0.15
~~~~~~

-  Process workers will kill themselves after attempting to process 100
   requests, instead of checking the internal queue 100 times.
-  If we find no messages on the internal queue, sleep for a moment
   before rechecking.

0.0.14
~~~~~~

-  Process workers will kill themselves after processing 100 requests
-  Process workers will check a message's fetch time and visibility
   timeout before processing, discarding it if it has exceeded the
   timeout.
-  Log the ``process_time()`` used to process a task to the INFO level.

0.0.13
~~~~~~

-  Only pass SQS Queue ID to internal queue. This is attempting to fix a
   bug when processing messages from multiple queues.

0.0.12
~~~~~~

-  Remove extraneous debugging code

0.0.11
~~~~~~

-  Add additional debugging to investigate message deletion errors

0.0.10
~~~~~~

-  Give each process worker its own boto connection to avoid
   multiprocess race conditions during message deletion

0.0.9
-----

-  Change long polling interval to a valid value, 0<=LPI<=20

0.0.8
-----

-  Switched to long polling when pulling down messages from SQS.
-  Moved message deletion from SQS until after message has been
   processed.

0.0.7
-----

-  Added capability to read JSON encoded celery messages.

0.0.6
-----

-  Switched shutdown logging to INFO
-  Added brief sleep to message retrieval loop so that we don't look
   like we are using a ton of CPU spinning.

0.0.5
-----

-  Switching task failure logging to ERROR (actually this time)
-  Moved task success logging to INFO
-  Added INFO level logging for number of messages retrieved from an SQS
   queue.
-  Moved Reader and Worker process counts to DEBUG

0.0.4
-----

-  Added ability to pass ``region``, ``access_key_id`` and
   ``secret_access_key`` through to Boto when creating connections
-  Switched logging of task failure to the ``ERROR`` logger, from
   ``INFO``.
