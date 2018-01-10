#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from argparse import ArgumentParser

from .worker import ManagerWorker
from . import __version__

logger = logging.getLogger("pyqs")


def main():
    parser = ArgumentParser(description="""
Run PyQS workers for the given queues
""")
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        dest="concurrency",
        default=1,
        help='Worker concurrency',
        action="store",
    )

    parser.add_argument(
        "queues",
        metavar="QUEUE_NAME",
        nargs="+",
        type=str,
        help='Queues to process',
        action="store",
    )

    parser.add_argument(
        "--loglevel",
        "--log_level",
        "--log-level",
        dest="logging_level",
        type=str,
        default="WARN",
        help='Set logging level. This must be one of the python default logging levels',
        action="store",
    )

    parser.add_argument(
        "--interval",
        dest="interval",
        type=float,
        default=0.0,
        help='Time waited by a worker after processesing a message.',
        action="store",
    )

    parser.add_argument(
        "--batchsize",
        dest="batchsize",
        type=int,
        default=10,
        help='How many messages to download at a time from SQS.',
        action="store",
    )

    parser.add_argument(
        "--prefetch-multiplier",
        dest="prefetch_multiplier",
        type=int,
        default=2,
        help='Multiplier on the size of the internal queue for prefetching SQS messages.',
        action="store",
    )

    args = parser.parse_args()

    _main(
        queue_prefixes=args.queues,
        concurrency=args.concurrency,
        logging_level=args.logging_level,
        interval=args.interval,
        batchsize=args.batchsize,
        prefetch_multiplier=args.prefetch_multiplier
    )


def _main(queue_prefixes, concurrency=5, logging_level="WARN", interval=1, batchsize=10, prefetch_multiplier=2):
    logging.basicConfig(format="[%(levelname)s]: %(message)s", level=getattr(logging, logging_level))
    logger.info("Starting PyQS version {}".format(__version__))
    manager = ManagerWorker(queue_prefixes, concurrency, interval, batchsize, prefetch_multiplier=prefetch_multiplier)
    manager.start()
    manager.sleep()
