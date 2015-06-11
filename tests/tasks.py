from pyqs import task

task_results = []


@task()
def index_incrementer(message, extra=None):
    if isinstance(message, basestring):
        task_results.append(message)
    else:
        raise ValueError("Need to be given basestring, was given {}".format(message))


@task(queue='foo')
def send_email(subject, message):
    print "{} {}".format(subject, message)
