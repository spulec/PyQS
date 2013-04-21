task_results = []


def index_incrementer(message):
    if isinstance(message, basestring):
        task_results.append(message)
    else:
        raise ValueError("Need to be given basestring, was given {}".format(message))
