## 0.0.9

* Change long polling interval to a valid value, 0<=LPI<=20

## 0.0.8

* Switched to long polling when pulling down messages from SQS.
* Moved message deletion from SQS until after message has been processed.

## 0.0.7

* Added capability to read JSON encoded celery messages.

## 0.0.6

* Switched shutdown logging to INFO
* Added brief sleep to message retrieval loop so that we don't look like we are using a ton of CPU spinning.

## 0.0.5

* Switching task failure logging to ERROR (actually this time)
* Moved task success logging to INFO
* Added INFO level logging for number of messages retrieved from an SQS queue.
* Moved Reader and Worker process counts to DEBUG

## 0.0.4

* Added ability to pass `region`, `access_key_id` and `secret_access_key` through to Boto when creating connections
* Switched logging of task failure to the `ERROR` logger, from `INFO`.
