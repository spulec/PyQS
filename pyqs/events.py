"""
pyqs events registry: register callback functions on pyqs events

Usage:
from pyqs.events import register_event

register_event("pre_process", lambda context: print(context))
"""


class Events:
    def __init__(self):
        self.pre_process = []
        self.post_process = []

    def clear(self):
        self.pre_process = []
        self.post_process = []


# Global singleton
_EVENTS = Events()


class NoEventException(Exception):
    pass


def register_event(name, callback):
    if hasattr(_EVENTS, name):
        getattr(_EVENTS, name).append(callback)
    else:
        raise NoEventException(f"{name} is not a valid pyqs event.")


def get_events():
    return _EVENTS


def clear_events():
    _EVENTS.clear()
