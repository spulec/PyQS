from pyqs import task

try:
    basestring
except NameError:
    basestring = str

task_results = []


@task()
def index_incrementer(message, extra=None):
    if isinstance(message, basestring):
        task_results.append(message)
    else:
        raise ValueError(
            "Need to be given basestring, was given {}".format(message))


@task()
def sleeper(message, extra=None):
    import time
    time.sleep(message)


@task(queue='email')
def send_email(subject, message):
    pass


@task(queue='delayed', delay_seconds=5)
def delayed_task():
    pass


@task(custom_function_path="custom_function.path", queue="foobar")
def custom_path_task():
    pass
