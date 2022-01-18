import sys
import atexit
import getpass
import argparse
import subprocess
from subprocess import PIPE

from utils.db import connect, close, execute_cmd


def parse_args():
    description = """Create simulations database"""
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--check',
                        action='store_true',
                        default=False,
                        help='Only check if the database exists')
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
                        type=int,
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

    return parser.parse_args()


def exists(conn, dbname):
    """
    Check if a database exists
    :param conn:
    :param dbname:
    :return:
    """
    cmd = f'SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower(\'{dbname}\');'
    out = execute_cmd(conn, cmd)
    return len(out) > 0


def create_db(args, password):
    """
    Create a new database.
    If the database exists this will create a prompt asking if the old database should be removed
    :param args:
    :param password:
    :return:
    """
    conn = connect('postgres', args.user, password, args.host, args.port)
    try:
        if exists(conn, dbname=args.dbname):
            print('DB exists')
            while True:
                overwrite = input(f'{args.dbname} exists.\nCreating a new database will erase the old.\nErase existing '
                                  f'database? (yes/no): ')
                if overwrite.lower() in ('', 'no'):
                    conn.close()
                    sys.exit(0)
                elif overwrite.lower() == 'yes':
                    # Drop database
                    cmd = f'PGPASSWORD=\'{password}\' dropdb {args.dbname} -h {args.host} -p {args.port} -U {args.user}'
                    out = subprocess.run(cmd, check=True, shell=True, stdout=PIPE, stderr=PIPE)
                    if out.returncode:
                        raise RuntimeError(f'Failed to delete existing database\n{out.stdout}\n{out.stderr}')
                    # Create database
                    cmd = f'PGPASSWORD=\'{password}\' createdb {args.dbname} -h {args.host} -p {args.port} -U {args.user} -O {args.user} -E \'UTF-8\''
                    out = subprocess.run(cmd, check=True, shell=True, stdout=PIPE, stderr=PIPE)
                    if out.returncode:
                        raise RuntimeError(f'Failed to create database\n{cmd}\n{out.stdout}\n{out.stderr}')
                    break
                else:
                    continue
        else:
            cmd = f'PGPASSWORD=\'{password}\' createdb {args.dbname} -h {args.host} -p {args.port} -U {args.user} -O {args.user} -E \'UTF-8\' '
            out = subprocess.run(cmd, check=True, shell=True, stdout=PIPE, stderr=PIPE)
            if out.returncode:
                raise RuntimeError(f'Failed to create database.\n{cmd}\n{out.stdout}\n{out.stderr}')
    except Exception as e:
        raise RuntimeError(e)


def populate_db(conn):
    # Create status lookup table

    cmd = 'CREATE TABLE sim_status_lookup (stat_name TEXT UNIQUE,' \
          ' id SMALLINT UNIQUE, ' \
          'description VARCHAR(1000));'
    out = execute_cmd(conn, cmd, fetch=False, commit=True)

    # Add status to table

    cmd = 'INSERT INTO sim_status_lookup(id, stat_name, description)' \
          'VALUES (0, \'failed\', \'simulation failed, see log for details\'),' \
          ' (1, \'submitted\', \'simulation submitted but not running\'),' \
          ' (2, \'running\', \'simulation running\'),' \
          ' (3, \'complete\', \'simulation completed\'),' \
          ' (4, \'depend\', \'Simulation depends on another simulation\'),' \
          ' (5, \'depend_failed\', \'Dependency failed\'),' \
          ' (6, \'updating\', \'Simulation parameters are being updated\');'

    execute_cmd(conn, cmd, fetch=False, commit=True)

    # Create worker table

    # This table registers all database workers, whether they are active, their (public) key and weight
    # Currently this table is not used but included to facilitate future updates

    cmd = 'CREATE TABLE worker (id INT UNIQUE GENERATED ALWAYS AS IDENTITY,' \
          ' host VARCHAR(40),' \
          ' port SMALLINT,' \
          ' active BOOLEAN,' \
          ' key BYTEA,' \
          ' weight SMALLINT );'

    execute_cmd(conn, cmd, fetch=False, commit=True)

    # Create sim table

    # The sim contains all unique simulations.
    # simulations are assigned a status from the sim_status_lookup table
    # A simulation can depend on another simulation specified in the "depend" columns

    cmd = 'CREATE TABLE sim (id INT UNIQUE GENERATED ALWAYS AS IDENTITY,' \
          ' stat_id SMALLINT NOT NULL,' \
          ' parent_id INT,' \
          'CONSTRAINT status_id_constrain ' \
          'FOREIGN KEY (stat_id) ' \
          'REFERENCES sim_status_lookup (id) ' \
          'ON DELETE SET NULL );'

    execute_cmd(conn, cmd, fetch=False, commit=True)

    # Create param table

    # The param table contains the simulation parameters in json format and the primary gmx command (e.g. grompp)
    # The table is a child of sim and all parameters are associated with a specific simulation
    # TODO Right now a simulation can have multiple param entries, is this something we want?
    cmd = 'CREATE TABLE param (id INT UNIQUE GENERATED ALWAYS AS IDENTITY, ' \
          'sim_id INT, ' \
          'path VARCHAR, ' \
          'cmd VARCHAR(16) NOT NULL, ' \
          'args JSONB NOT NULL, ' \
          'CONSTRAINT sim ' \
          'FOREIGN KEY(sim_id) ' \
          'REFERENCES sim(id) ' \
          'ON DELETE CASCADE);'
    execute_cmd(conn, cmd, fetch=False, commit=True)

    # Create fout table

    # The fout table keep track of output files
    # Output files are stored in json format {"file_type": "/path/to/file"}
    # If a filetype is not recognized it is stored as unkn_<i> where <i> is a simple numeric index
    # TODO Right now a simulation can have multiple fout entries, is this something we want?

    cmd = 'CREATE TABLE fout (id INT UNIQUE GENERATED ALWAYS AS IDENTITY, ' \
          'sim_id INT, ' \
          'files JSONB, ' \
          'CONSTRAINT sim ' \
          'FOREIGN KEY(sim_id) ' \
          'REFERENCES sim(id) ' \
          'ON DELETE CASCADE);'
    execute_cmd(conn, cmd, fetch=False, commit=True)

    # Create slurm table

    # The queue_info table contains the job_id of a simulation running on a queuing system

    cmd = 'CREATE TABLE job_info (id INT UNIQUE GENERATED ALWAYS AS IDENTITY, ' \
          'sim_id INT, ' \
          'job_id INT UNIQUE, ' \
          'CONSTRAINT sim ' \
          'FOREIGN KEY(sim_id) ' \
          'REFERENCES sim(id) ' \
          'ON DELETE CASCADE);'
    execute_cmd(conn, cmd, fetch=False, commit=True)

    return


def main(args):
    # get password
    if args.password is None:
        password = getpass.getpass()
    else:
        password = args.password

    if args.check:
        conn = connect('postgres', args.user, password, args.host, args.port)
        atexit.register(close, conn)
        if exists(conn, args.dbname):
            print(f'{args.dbname} exists')
        else:
            print(f'{args.dbname} doe not exist')
        sys.exit(0)
    else:
        create_db(args, password)
        conn = connect(args.dbname, args.user, password, args.host, args.port)
        atexit.register(close, conn)
        populate_db(conn)


if __name__ == '__main__':
    args = parse_args()
    main(args)
