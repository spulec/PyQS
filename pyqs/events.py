class Events:
    def __init__(self):
        self.pre_process = []
        self.post_process = []


# Singleton
EVENTS = Events()


def register(name, callback):
    if hasattr(EVENTS, name):
        getattr(EVENTS, name).append(callback)
    else:
        raise Exception(f"{name} is not a valid pyqs event.")


def get_events():
    return EVENTS
