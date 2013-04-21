import os
import sys

# Add the current directory to sys.path so we can import the settings from it
test_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, test_dir)
os.environ["DJANGO_SETTINGS_MODULE"] = 'settings'

from django.core.management import call_command

from mock import patch
from moto import mock_sqs


@patch("pyqs.management.commands.pyqs_worker._main")
@mock_sqs
def test_management_command(pyqs_worker_main):
    call_command("pyqs_worker", "email1", "email2", concurrency=2)

    pyqs_worker_main.assert_called_once_with(queue_prefixes=['email1', 'email2'], concurrency=2)
