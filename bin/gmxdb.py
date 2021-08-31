import os
import getpass
import argparse
import logging
from logging.handlers import QueueListener
from multiprocessing import Queue

from db_worker import DatabaseWorkerMain

def parse_args():
    description = """Run database daemon"""
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-q',
                        '--queue',
                        type=str,
                        default=None,
                        help='The queuing system, if left blank will try to determine automatically')

    parser.add_argument('--clean',
                        default=False,
                        action='store_true',
                        help='If true, remove jobscripts and logs when jobs finish, this is recommended when running '
                             'many of simulations')

    # Database specific arguments
    parser.add_argument('-d',
                        '--dbname',
                        type=str,
                        default='gmx',
                        help='database name to connect to'
                        )
    parser.add_argument('-U',
                        '--user',
                        type=str,
                        default=getpass.getuser(),
                        help='database user name')
    parser.add_argument('-W',
                        '--password',
                        type=str,
                        default=None,
                        help='database password, will open password prompt if left blank')
    parser.add_argument('--host',
                        type=str,
                        default='localhost',
                        help='database server host or socket directory')
    parser.add_argument('-p',
                        '--port',
                        type=int,
                        default=9987,
                        help='database server port')

    # Logging specific arguments
    parser.add_argument('-v',
                        '--verbose',
                        default=False,
                        action='store_true',
                        help='Provide extra information, setting log level to DEBUG')
    return parser.parse_args()


def configure_logger(level):
    root = logging.getLogger()
    root.addHandler(logging.StreamHandler())
    root.setLevel(level)


def main(args):
    if args.password is None:
        password = getpass.getpass()
    elif os.path.isfile(args.password):
        with open(args.password, 'r') as fh:
            password = fh.readline().rstrip('\n')
    else:
        password = args.password

    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    configure_logger(level)

    # Set up "basic" logger
    handler = logging.FileHandler('/home/fleidne/gmxdb/gmxdb.log')
    formatter = logging.Formatter('{asctime} - {process} - {levelname} - {message}', style='{')
    handler.setFormatter(formatter)
    handler.setLevel(level)
    q = Queue()
    listener = QueueListener(q, handler)
    listener.start()

    dbw = DatabaseWorkerMain(args.dbname, args.user, password, args.host, args.port, log_queue=q, clean=args.clean)
    dbw.run()
    dbw.join()


if __name__ == '__main__':
    args = parse_args()
    main(args)
