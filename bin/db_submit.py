import os
import json
import atexit
import getpass
import argparse
import time

from utils.db import connect, close, execute_cmd


def parse_args():
    description = """Submit one or more gromacs jobs to the database"""
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--cfg',
                        type=str,
                        default=None,
                        help='A configuration file with instructions for one or more simulation')
    parser.add_argument('--cmd',
                        type=str,
                        default=None,
                        help='A gmx command. See "$gmx help commands" for a list of available options')
    parser.add_argument('--args',
                        type=str,
                        default=None,
                        help='A number of keyword arguments passed to the gromacs command as a file or json readable '
                             'string')
    parser.add_argument('--dependency',
                        type=int,
                        default=0,
                        help='sim_id of another simulation. Will delay running the simulation until dependency is '
                             'complete')
    parser.add_argument('--base',
                        type=str,
                        default=os.path.abspath('../'),
                        help='The simulation base directory. By default the current directory is used.')
    parser.add_argument('--wait',
                        default=False,
                        action='store_true',
                        help='If true wait until the job status changes either to complete, failed or depend_failed')

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

    return parser.parse_args()


def register(conn, gmx_cmd, gmx_args, fout=None, depend=0, base='./'):
    """
    Register a simulation with the database
    :param conn:
    :param gmx_cmd:
    :param gmx_args:
    :param fout: User defined outfiles
    :type fout: dict
    :param depend: id of depend simulation
    :param base: A path or environment variable
    :return:
    """

    # Get base path
    if base is None:
        base = os.path.abspath('./')
    elif base[0] == '$':
        base = os.environ[base[1:]]

    if not os.path.isabs(base):
        base = os.path.abspath(base)

    # Add an entry for the simulation, flag it as updating, so that no process accesses it.
    # If simulation has a dependency: provide the parent_id
    if depend:
        cmd = f'INSERT INTO sim(stat_id, parent_id) VALUES(6, {depend}) RETURNING id;'
        sim_id = execute_cmd(conn, cmd, commit=True, fetch=True)[0][0]
    else:
        cmd = 'INSERT INTO sim(stat_id) VALUES(6) RETURNING id;'
        sim_id = execute_cmd(conn, cmd, commit=True, fetch=True)[0][0]

    # Populate params
    #  FIXME Sometimes a horrible oneliner is not the best solutions (sometimes)
    json_str = '{'+','.join([f'"{kw}": "{arg}"' if isinstance(arg, str) else f'"{kw}": {arg}' for kw, arg in gmx_args.items()])+ ' }'
    cmd = f'INSERT INTO param(sim_id, path, cmd, args) VALUES({sim_id}, \'{base}\', \'{gmx_cmd}\', \'{json_str}\');'
    execute_cmd(conn, cmd, commit=True, fetch=False)

    # Add user defined outfiles
    if fout is not None:
        cmd = f'INSERT INTO fout(sim_id, files) VALUES({sim_id}, \'{json.dumps(fout)}\');'
        execute_cmd(conn, cmd, commit=True, fetch=False)

    # Flag simulation as submitted/depend
    if depend:
        cmd = f'UPDATE sim SET stat_id = 4 WHERE id={sim_id};'
        execute_cmd(conn, cmd, commit=True, fetch=False)
    else:
        cmd = f'UPDATE sim SET stat_id = 1 WHERE id={sim_id};'
        execute_cmd(conn, cmd, commit=True, fetch=False)
    return sim_id


def wait(sim_ids, conn, interval=2):
    """
    Wait till all simulation have either completed or failed
    :param sim_ids:
    :param conn:
    :param interval:
    :return:
    """
    running = [True, ]*len(sim_ids)
    while any(running):
        for i, sid in enumerate(sim_ids):
            if running[i]:
                cmd = f'SELECT stat_id FROM sim WHERE id={sid};'
                with conn.cursor() as cursor:
                    cursor.execute(cmd)
                    stat_id = cursor.fetchone()[0]
                if stat_id in (0, 3, 5):  # Stat codes for failed, complete & depend_failed
                    running[i] = False
        time.sleep(interval)
    return


def main(args):
    if args.password is None:
        password = getpass.getpass()
    elif os.path.isfile(args.password):
        with open(args.password, 'r') as fh:
            password = fh.readline().rstrip('\n')
    else:
        password = args.password

    # Load config, if applicable add commandline options last
    cfg = []
    if args.cfg is not None:
        with open(args.cfg, 'rb') as fh:
            cfg = json.load(fh)

    if all([args.cmd is not None, args.args is not None]):
        cfg.append({'cmd': args.cmd,
                    'args': args.args,
                    'base': args.base,
                    'dependency': args.dependency
                    })

    # Connect to database
    conn = connect(args.dbname, args.user, password, args.host, args.port)
    atexit.register(close, conn)

    sim_ids = []
    for i, stage in enumerate(cfg):
        dependency = stage.get('dependency')
        if dependency is not None and dependency < 0:
            try:
                dependency = sim_ids[dependency]
            except ValueError:
                print(f'Dependency for stage {i} could not be met: {dependency}')
        elif dependency is None:
            dependency = 0
        _id = register(conn, stage['cmd'], stage['args'], fout=stage.get('fout'), depend=dependency,
                       base=stage.get('base'))
        sim_ids.append(_id)
    if args.wait:
        wait(sim_ids, conn)
        for sid in sim_ids:
            print(sid)


if __name__ == '__main__':
    args = parse_args()

    if args.cfg is None and any([args.cmd is None, args.args is None]):
        raise ValueError('Must provide either a config file or a set of commandline instructions')

    main(args)
