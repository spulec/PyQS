from pyqs import task

task_results = []


@task()
def index_incrementer(message, extra=None):
    if isinstance(message, basestring):
        task_results.append(message)
    else:
        raise ValueError("Need to be given basestring, was given {}".format(message))


@task(queue='email')
def send_email(subject, message):
    pass
