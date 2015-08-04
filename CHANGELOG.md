## 0.0.5

* Switching task failure logging to ERROR (actually this time)
* Moved task success logging to INFO
* Added INFO level logging for number of messages retrieved from an SQS queue.
* Moved Reader and Worker process counts to DEBUG

## 0.0.4

* Added ability to pass `region`, `access_key_id` and `secret_access_key` through to Boto when creating connections
* Switched logging of task failure to the `ERROR` logger, from `INFO`.
