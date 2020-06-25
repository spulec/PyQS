# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from functools import wraps

from threading import Thread

from pyqs import events


class MockLoggingHandler(logging.Handler):
    """Mock logging handler to check for expected logs."""

    def __init__(self, *args, **kwargs):
        self.reset()
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.messages[record.levelname.lower()].append(record.getMessage())

    def reset(self):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': [],
        }


class ThreadWithReturnValue2(Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={},
                 Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs, Verbose)
        self._return = None

    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(
                *self._Thread__args, **self._Thread__kwargs)

    def join(self):
        Thread.join(self)
        return self._return


class ThreadWithReturnValue3(Thread):
    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None, daemon=None):
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self):
        Thread.join(self)
        return self._return


def clear_events_registry(fn):
    """Clear the global events registry before each test."""

    @wraps(fn)
    def wrapper(*args, **kwargs):
        events.clear_events()
        return fn(*args, **kwargs)
    return wrapper
