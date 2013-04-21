import multiprocessing
import os
import signal
import sys

# Add the current directory to sys.path so we can import the settings from it
test_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, test_dir)
os.environ["DJANGO_SETTINGS_MODULE"] = 'settings'

from django.core.management import call_command

from moto import mock_sqs


@mock_sqs
def test_management_command():
    call_command("pyqs_worker", "email1", "email2", concurrency=2)

    children = multiprocessing.active_children()
    for child in children:
        os.kill(child.pid, signal.SIGTERM)
