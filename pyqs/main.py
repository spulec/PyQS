#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import os
import sys
from argparse import ArgumentParser

from .worker import ManagerWorker, SimpleManagerWorker
from . import __version__

logger = logging.getLogger("pyqs")

SIMPLE_WORKER_DEFAULT_BATCH_SIZE = 1
DEFAULT_BATCH_SIZE = 10


def _set_batchsize(args):
    batchsize = args.batchsize
    if batchsize:
        return batchsize

    simple_worker = args.simple_worker
    if simple_worker:
        # Default batchsize for SimpleProcessWorker
        return SIMPLE_WORKER_DEFAULT_BATCH_SIZE

    # Default batchsize for ProcessWorker
    return DEFAULT_BATCH_SIZE


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
        help=(
            'Set logging level. '
            'This must be one of the python default logging levels'
        ),
        action="store",
    )

    parser.add_argument(
        "--access-key-id",
        dest="access_key_id",
        type=str,
        default=None,
        help='AWS_ACCESS_KEY_ID used by Boto',
        action="store",
    )

    parser.add_argument(
        "--secret-access-key",
        dest="secret_access_key",
        type=str,
        default=None,
        help='AWS_SECRET_ACCESS_KEY used by Boto',
        action="store",
    )

    parser.add_argument(
        "--region",
        dest="region",
        type=str,
        default=None,
        help='AWS Region to connect to SQS',
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
        default=None,
        help='How many messages to download at a time from SQS.',
        action="store",
    )

    parser.add_argument(
        "--prefetch-multiplier",
        dest="prefetch_multiplier",
        type=int,
        default=2,
        help=(
            'Multiplier on the size of the internal queue '
            'for prefetching SQS messages.'
        ),
        action="store",
    )

    parser.add_argument(
        '--simple-worker',
        dest='simple_worker',
        default=False,
        action='store_true'
    )

    args = parser.parse_args()

    _main(
        queue_prefixes=args.queues,
        concurrency=args.concurrency,
        logging_level=args.logging_level,
        region=args.region,
        access_key_id=args.access_key_id,
        secret_access_key=args.secret_access_key,
        interval=args.interval,
        batchsize=_set_batchsize(args),
        prefetch_multiplier=args.prefetch_multiplier,
        simple_worker=args.simple_worker
    )


def _add_cwd_to_path():
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)


def _main(queue_prefixes, concurrency=5, logging_level="WARN",
          region=None, access_key_id=None, secret_access_key=None,
          interval=1, batchsize=DEFAULT_BATCH_SIZE, prefetch_multiplier=2,
          simple_worker=False):
    logging.basicConfig(
        format="[%(levelname)s]: %(message)s",
        level=getattr(logging, logging_level),
    )
    logger.info("Starting PyQS version {}".format(__version__))

    if simple_worker:
        manager = SimpleManagerWorker(
            queue_prefixes, concurrency, interval, batchsize,
            region=region, access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )
    else:
        manager = ManagerWorker(
            queue_prefixes, concurrency, interval, batchsize,
            prefetch_multiplier=prefetch_multiplier, region=region,
            access_key_id=access_key_id, secret_access_key=secret_access_key,
        )

    _add_cwd_to_path()
    manager.start()
    manager.sleep()
