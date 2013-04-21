from optparse import make_option

from django.core.management.base import BaseCommand

from pyqs.worker import _main


class Command(BaseCommand):
    args = '<queue_name queue_name ...>'
    help = 'Runs PyQS workers for the given queues'

    option_list = BaseCommand.option_list + (
        make_option(
            "-c",
            "--concurrency",
            type="int",
            dest="concurrency",
            default=1,
            help='Worker concurrency'
        ),
    )

    def handle(self, *args, **kwargs):
        queues = [queue for queue in args]
        concurrency = kwargs['concurrency']

        _main(queue_prefixes=queues, concurrency=concurrency)
