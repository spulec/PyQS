import argparse

from pyqs.worker import _main


def main():
    parser = argparse.ArgumentParser(description="""
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

    args = parser.parse_args()

    _main(queue_prefixes=args.queues, concurrency=args.concurrency)


if __name__ == '__main__':
    main()
