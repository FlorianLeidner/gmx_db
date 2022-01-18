import os
import getpass
import argparse
import logging
import time
import signal
from logging.handlers import QueueListener
from multiprocessing import Queue, Event

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
    parser.add_argument('--log_dir',
                        type=str,
                        default=os.getcwd(),
                        help='Logfile directory, default=pwd')

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


def handler(signalname):
    def f(sighnal_recieved, frame):
        raise KeyboardInterrupt(f"{signalname} received")
    return f


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
    handler = logging.FileHandler(os.path.join(args.log_dir, 'gmxdb.log'))
    formatter = logging.Formatter('{asctime} - {process} - {levelname} - {message}', style='{')
    handler.setFormatter(formatter)
    handler.setLevel(level)
    q = Queue()
    listener = QueueListener(q, handler)
    listener.start()

    stop_event = Event()
    dbw = DatabaseWorkerMain(args.dbname, args.user, password, args.host, args.port, stop_event=stop_event, log_queue=q,
                             clean=args.clean)

    dbw.start()

    # If interrupted set a stop_event to "gracefully" terminate all workers
    try:
        dbw.join()
    except KeyboardInterrupt:
        # TODO Feed this message to a proper logger
        print("Caught KeyboardInterrupt! Setting stop event...")
    finally:
        # Here we shut down the main worker by setting the stop event
        # This can take a few seconds so we give it a grace period of 10 seconds
        # After the grace period we terminate the process with SIGKILL
        grace_period = 10.
        stop_event.set()
        t = time.time()
        while dbw.is_alive():
            if time.time() > t+grace_period:
                dbw.kill()


if __name__ == '__main__':

    # Before configuring the MainWorker we set the handler for SIGINT and SIGTERM
    # NOTE:  The signal handler will only be inherited by the worker if it is forked (not spawned)
    # NOTE: Right now (Python 3.8) this is the default behavior but might change in the future
    signal.signal(signal.SIGINT, handler("SIGINT"))
    signal.signal(signal.SIGTERM, handler("SIGTERM"))
    args = parse_args()
    main(args)
