import json
import time
import logging
import multiprocessing
from logging.handlers import QueueHandler

from utils.db import connect, execute_cmd
from utils.hpcc import JobStatus
from utils.gmx import *

from multiprocessing import Process, Queue


class DatabaseWorker(Process):
    """
    The Base class for database workers
    """
    def __init__(self, dbname, user, password, host, port, log_queue=None):
        super().__init__()
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        # Set up the logger
        if log_queue is not None:
            self.configure_logger(log_queue)
            self.name = multiprocessing.current_process().name
            self.logger = logging.getLogger(self.name)

    def db_exec(self, cmd, max_attempt=10, interval=2, fetch=True, commit=False):
        """
        Perform a database query
        Because there is a maximum to the number of simultaneous connections  we:
        A) Only open connections when needed
        B) Repeat failed connection attempt
        :param cmd: A postgresql query
        :param max_attempt: how often to try to connect to db
        :param interval: how long to wait between attempts
        :param fetch
        :param commit
        :return:
        """

        for _ in range(max_attempt):
            try:
                conn = connect(self.dbname, self.user, self.password, self.host, self.port)
                try:
                    if fetch:
                        out = execute_cmd(conn, cmd, fetch=fetch, commit=commit)
                        return out
                    else:
                        execute_cmd(conn, cmd, fetch=fetch, commit=commit)
                        return
                finally:
                    conn.close()
            except Exception as e:
                print(f'Failed to connect to database with {e}')
                time.sleep(interval)
                continue

    def configure_logger(self, queue):
        """
        Set up logging
        :param queue:
        :return:
        """
        handler = QueueHandler(queue)
        root = logging.getLogger()
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)


class DatabaseWorkerMain(DatabaseWorker):

    def __init__(self, dbname, user, password, host, port, stop_event, interval=10, timeout=-1, log_queue=None, clean=False):
        """
        Monitor jobs on a database and assign Monitor Workers to running jobs

        :param dbname:
        :param user:
        :param password:
        :param host:
        :param port:
        :param interval: How much time between queries
        :param timeout: timeout in seconds, negative values will run until terminated
        :param log_queue: A queue used for logging
        :param clean: If true, try to delete jobscripts and log files when a job completes
        """
        self.log_queue = log_queue
        self.db_info = (dbname, user, password, host, port)
        super().__init__(*self.db_info, log_queue=self.log_queue)

        self.stop_event = stop_event
        self.interval = interval
        self.timeout = timeout
        self.queue = Queue()
        self.clean = clean

    def is_valid(self, sim_id, stat_id):
        """
        Check if a job is valid
        If it's stat_id is 1 (Submitted) it must have a param entry
        If it's stat_id is 4 (depend) is must have a param entry and a valid parent_id
        :param sim_id:
        :param stat_id:
        :return:
        """

        # Both stat_id 1 & 4 must have a param entry
        cmd = f'SELECT * FROM param WHERE sim_id={sim_id}'
        out = self.db_exec(cmd, fetch=True, commit=False)
        if out is None or len(out) == 0:
            self.logger.error(f'Could not find simulation parameters for {sim_id}')
            self.logger.error(f'Parameters contains: {out}')
            return False
        else:
            # stat_id 4 also requires a valid parent
            if stat_id == 4:
                cmd = f'SELECT parent_id FROM sim WHERE id={sim_id}'
                out = self.db_exec(cmd, fetch=True, commit=False)
                parent_id = out[0][0]
                cmd = f'SELECT parent_id FROM sim WHERE id={parent_id}'
                out = self.db_exec(cmd, fetch=True, commit=False)
                if out is None or len(out) == 0:
                    self.logger.error(f'Could not find parent simulation for {sim_id}')
                else:
                    return True
            else:
                return True

    def run(self):
        self.logger.info(f'Started Main worker with name: {self.name}')
        # Keep track of all sim_ids with active workers
        active = {}
        t = time.time()
        while 1:
            if self.stop_event.is_set():
                break
            # First check if any workers
            while not self.queue.empty():
                sim_id = self.queue.get()
                self.logger.debug(f'Received exit code for: {sim_id}')
                active[sim_id].join()  # Wait until the worker shuts down
                del(active[sim_id])
                self.logger.debug(f'Recycled worker for: {sim_id}')
            # Get all simulations flagged as either submitted, running or depend
            query = 'SELECT id, stat_id FROM sim WHERE stat_id=1 OR stat_id=2 OR stat_id=4;'
            out = self.db_exec(query, fetch=True, commit=False)
            for (sim_id, stat_id) in out:
                # Skip all jobs that already  have a worker assigned
                if sim_id in active.keys():
                    continue
                # For submitted jobs and jobs with dependency check if they are valid
                if stat_id in (1, 4):
                    self.logger.debug(f'No worker assigned to: {sim_id} with stat_id: {stat_id}')
                    valid = self.is_valid(sim_id, stat_id)
                    if not valid:
                        self.logger.error(f'sim_id: {sim_id} not a valid job, flagging as failed')
                        cmd = f'UPDATE sim SET stat_id=0 WHERE id={sim_id};'
                        self.db_exec(cmd, fetch=False, commit=True)
                        continue

                # Launch workers according to stat_id
                if stat_id == 1:  # Submitted
                    self.logger.debug(f'Launching GMXSubmit worker for {sim_id}')
                    active[sim_id] = GMXSubmit(*self.db_info, sim_id=sim_id, queue=self.queue, log_queue=self.log_queue,
                                               ntrials=3)
                    active[sim_id].daemon = True
                    active[sim_id].start()
                elif stat_id == 2:  # Running
                    self.logger.debug(f'Launching Monitor worker for {sim_id}')
                    active[sim_id] = Monitor(*self.db_info, sim_id=sim_id, queue=self.queue, log_queue=self.log_queue,
                                             clean=self.clean)
                    active[sim_id].daemon = True
                    active[sim_id].start()
                elif stat_id == 4:  # depend
                    self.logger.debug(f'Launching Depend worker for {sim_id}')
                    active[sim_id] = Depend(*self.db_info, sim_id=sim_id, queue=self.queue)
                    active[sim_id].daemon = True
                    active[sim_id].start()


class Monitor(DatabaseWorker):
    """
    Monitor a running job
    """
    def __init__(self, dbname, user, password, host, port, sim_id, queue, interval=5, timeout=-1, log_queue=None,
                 clean=False):
        """

        :param dbname:
        :param user:
        :param password:
        :param host:
        :param port:
        :param sim_id
        :param queue: Once the job is no longer running we inform DataBaseWorkerMain
        :param interval:
        :param timeout:
        :param clean: If true, remove jobscripts and logfiles if job exits with status 3 (completed)
        """
        # Init parent class
        super().__init__(dbname, user, password, host, port, log_queue=log_queue)
        self.sim_id = sim_id
        self.queue = queue
        self.interval = interval
        self.timeout = timeout
        # Get a list of job ids associated with the sim_id
        _ids = [q[0] for q in self.db_exec(f'SELECT job_id FROM job_info WHERE sim_id={sim_id};')]
        self.js = JobStatus(_ids)
        self.clean = clean

    def cleanup(self):
        """
        Delete JOSCRIPTS and JLOGS
        :return:
        """

        cmd = f'SELECT files FROM fout WHERE sim_id={self.sim_id}'
        file_dict = self.db_exec(cmd, fetch=True, commit=False)[0][0]

        jscripts = file_dict.get('JSCRIPTS')
        if jscripts is None:
            self.logger.warning('Cleanup was called but could not find any JSCRIPTS')
        else:
            for fn in file_dict['JSCRIPTS']:
                if not os.path.isfile(fn):
                    self.logger.warning(f'Cleanup could not find: {fn} for sim_id: {self.sim_id}')
                else:
                    self.logger.debug(f'Removing {fn}')
                    os.remove(fn)
            del(file_dict['JSCRIPTS'])
        jlogs = file_dict.get('JLOGS')
        if jlogs is None:
            self.logger.warning('Cleanup was called, but could not find any JLOGS')
        else:
            for fn in file_dict['JLOGS']:
                if not os.path.isfile(fn):
                    self.logger.warning(f'Cleanup could not find: {fn} for sim_id: {self.sim_id}')
                else:
                    self.logger.debug(f'Removing {fn}')
                    os.remove(fn)
            del(file_dict['JLOGS'])

        cmd = f'UPDATE fout SET files=\'{json.dumps(file_dict)}\' WHERE sim_id={self.sim_id};'
        self.db_exec(cmd, fetch=False, commit=True)
        return

    #  TODO Should check for outfiles once the job completes
    def run(self):
        self.logger.debug(f'Monitoring: {self.sim_id}')
        t = time.time()
        while self.timeout*(time.time()-t) < self.timeout**2:
            status = self.js.status
            if status != 2:
                if status == 0:
                    self.logger.error(f'{self.sim_id} no longer running; FAILED with Stat_id: {status}')
                else:
                    self.logger.debug(f'{self.sim_id} no longer running. Stat_id: {status}')
                if status == 3 and self.clean:
                    self.cleanup()

                cmd = f'UPDATE sim SET stat_id={status} WHERE id={self.sim_id}'
                self.db_exec(cmd, fetch=False, commit=True)
                self.logger.debug(f'Changed job status to: {status}')
                self.queue.put(self.sim_id)
                return
            time.sleep(self.interval)


class GMXSubmit(DatabaseWorker):
    def __init__(self, dbname, user, password, host, port, sim_id, queue, log_queue=None, ntrials=1):
        """
        Submit a simulation with g_submit
        :param dbname:
        :param user:
        :param password:
        :param host:
        :param port:
        :param sim_id:
        :param queue:
        :param ntrials: How often to try to run the job before returning failed
        """
        # Init parent class
        super().__init__(dbname, user, password, host, port, log_queue)

        self.sim_id = sim_id
        self.queue = queue

        # Get job information
        self.app = self.get_app()

        # sim_id of the hypothetical dependency (Will be None if None
        self.depend = self.get_dependency(sim_id=self.sim_id)

        if self.depend is not None:
            self.depend_fout = self.get_fout(self.depend)
        else:
            self.depend_fout = None

        # Parse arguments resolving dependencies
        self.args = None

        self.ntrials = ntrials

    def get_app(self):
        """
        Get application
        :return:
        """
        cmd = f'SELECT cmd FROM param WHERE sim_id={self.sim_id}'
        out = self.db_exec(cmd, fetch=True, commit=False)
        return out[0][0]

    def get_args(self):
        """
        Get the arguments passed to a job
        :return:
        """
        cmd = f'SELECT args from param WHERE sim_id={self.sim_id}'
        out = self.db_exec(cmd, fetch=True, commit=False)
        return out[0][0]

    def get_base(self):
        """
        Get the arguments passed to a job
        :return:
        """
        cmd = f'SELECT path from param WHERE sim_id={self.sim_id}'
        out = self.db_exec(cmd, fetch=True, commit=False)
        return out[0][0]

    def get_dependency(self, sim_id):
        """
        Get the sim_id of a dependency, will return None if no dependency
        :param sim_id: The simulation id
        :return:
        """
        cmd = f'SELECT parent_id FROM sim WHERE id={sim_id};'
        out = self.db_exec(cmd, fetch=True, commit=False)
        return out[0][0]

    def get_fout(self, sim_id, depend_key='%'):
        """
        Get output files for a specific sim_id
        If output files are inherited, recurse until inheritance is resolved
        :param sim_id:
        :param depend_key: Character indicating that the file is inherited from parent
        :return:
        """
        cmd = f'SELECT files FROM fout WHERE sim_id={sim_id};'
        out = self.db_exec(cmd, fetch=True, commit=False)
        if len(out) == 0:
            return {}
        else:
            update = False
            fout = out[0][0]
            for ft, fn in fout.items():
                if fn[0] == depend_key:
                    # Get the sim_id of the parent
                    parent_id = self.get_dependency(sim_id=sim_id)
                    parent_fout = self.get_fout(sim_id=parent_id)
                    fn = parent_fout.get(fn[1:])
                    fout[ft] = fn
                    update = True
            # If we encounter a dependency update the database to prevent recursion madness
            if update:
                cmd = f'INSERT INTO fout(files) VALUES(\'{json.dumps(fout)}\') WHERE sim_id={sim_id};'
                self.db_exec(cmd, fetch=False, commit=True)
            return fout

    def parse_args(self, raw_args, depend_key='%', base='./'):
        """
        Parse arguments getting files that are inherited from dependency
        :param raw_args:
        :param depend_key: single symbol key indicating a dependency
        :param base:
        :return:
        """
        file_args = {'g_submit': ('-s', '-cpi', '-ei', '-table', '-tabletf',
                                  '-tablep', '-tableb', '-o', '-eo', '-deffnm'),
                     'grompp': ('-f', '-c', '-r', '-rb', '-n', '-p', '-t', '-e',
                                '-ref', '-po', '-pp', '-o', '-imd'),
                     'shell': ()}
        args = {}
        for kw, arg in raw_args.items():
            if isinstance(arg, str) and len(arg) > 0 and arg[0] == depend_key:
                if self.depend_fout is not None:

                    if arg[1:] not in self.depend_fout:
                        self.logger.error(f'Simulation depends on output files: {kw}, but file type not found in '
                                          f'outfiles: {self.depend_fout}')
                        return
                    else:
                        args[kw] = self.depend_fout[arg[1:]]
                else:
                    self.logger.error(f'Simulation depends on output files: {kw}, but no dependency outfiles found in '
                                      f'database')
                    return
            else:
                if kw in file_args[self.app]:
                    if not os.path.isabs(arg):
                        arg = os.path.join(base, os.path.basename(arg))
                args[kw] = arg

        return args

    def set_fout(self, out):
        """
        Get a dictionary containing all (potential) outfiles
        :param out: The stdout produced by the job, required to set JSCRIPTS & JLOGS
        :return:
        """
        # Get user defined outfiles
        outfiles = self.get_fout(self.sim_id)

        update_func = {'g_submit': gsubmit_out, 'grompp': grompp_out, 'shell': shell_out}[self.app]

        outfiles = update_func(self.args, base=self.get_base(), outfiles=outfiles)

        # For g_submit we also need to get the jobscripts and joblogs (JSCRIPTS, JLOGS)
        if self.app == 'g_submit':
            outfiles.update(gsubmit_auxfiles(out))

        cmd = f'INSERT INTO fout(sim_id, files) VALUES({self.sim_id}, \'{json.dumps(outfiles)}\');'
        self.db_exec(cmd, fetch=False, commit=True)
        return

    def set_status(self, stat_id):
        """
        Set simulation status
        :param stat_id:
        :return:
        """
        cmd = f'UPDATE sim SET stat_id = {stat_id} WHERE id = {self.sim_id};'
        self.db_exec(cmd, fetch=False, commit=True)
        if int(stat_id) in (2, 3):  # Status: Complete/Running
            self.logger.debug(f'Updated stat_id for {self.sim_id} to: {stat_id}')
        else:
            self.logger.error(f'Updated stat_id for {self.sim_id} to: {stat_id}')

    def run(self):
        self.logger.debug(f'Running {self.app} on {self.sim_id}')

        # Get arguments
        raw_args = self.get_args()
        self.args = self.parse_args(raw_args, base=self.get_base())

        if self.args is None:  # This can happen if a file dependency is not met
            self.set_status(0)
            return
        # What function to use for submitting the job
        submit_func = {'g_submit': gsubmit_run, 'grompp': grompp_run, 'shell': shell_run}[self.app]

        trials = 0
        return_code = 0  # Defined to turn of warning in pycharm
        while trials < self.ntrials:
            return_code, out = submit_func(self.args)
            if return_code:
                trials += 1
                self.logger.warning(f'Failed to run {self.app} for sim_id: {self.sim_id}. Trial:: {trials}/{self.ntrials}')
                time.sleep(5)
                continue
            else:
                break

        if return_code:
            self.logger.error(f'{self.app} returned error code {return_code}\n{out}')
            self.set_status(0)  # Set status: Failed
        else:
            # Commit (preliminary) outfiles to database
            self.set_fout(out)

            # If jobs were submitted to the cluster add them to job_info
            if self.app == 'g_submit':
                # Get job ids
                batch_ids = gsubmit_batch_ids(out)
                for job_id in batch_ids:
                    cmd = f'INSERT INTO job_info(sim_id, job_id) VALUES ({self.sim_id}, {job_id});'
                    self.db_exec(cmd, fetch=False, commit=True)
                self.set_status(2)  # Set status to Running
            elif self.app in ('grompp', 'shell'):
                self.set_status(3)  # Set status Complete
        # Send signal to head worker to garbage collect
        self.queue.put(self.sim_id)
        return


class Depend(DatabaseWorker):
    """
    Similar to Monitor Running, but monitors a dependency.

    This worker is triggered by simulations with stat_id: 4 (depend)
    The worrker will periodically query the database for the dependency.

    When the stat_id of the dependency changes to 3 (completed)
    the worker will change the stat_id of the child simulation to 1 (submitted)

    If the stat_id of the parent changes to 0 or 5 (failed, dependency failed)
    the worker will change the stat_id of the child to 5 (dependency failed)
    """
    def __init__(self, dbname, user, password, host, port, sim_id, queue, interval=5, timeout=-1):
        """

        :param dbname:
        :param user:
        :param password:
        :param host:
        :param port:
        :param sim_id:
        :param queue:
        :param interval:
        :param timeout:
        """
        super().__init__(dbname, user, password, host, port)
        self.sim_id = sim_id
        self.queue = queue
        self.interval = interval
        self.timeout = timeout
        # Get parent id
        self.parent_id = self.get_parent_id()

    def get_parent_id(self):
        cmd = f'SELECT parent_id FROM sim WHERE id = {self.sim_id};'
        out = self.db_exec(cmd, fetch=True, commit=False)
        return out[0][0]

    def get_parent_stat_id(self, pid):
        """
        Get the status of the parent
        :param pid:
        :return:
        """
        cmd = f'SELECT stat_id FROM sim WHERE id = {pid};'
        out = self.db_exec(cmd, fetch=True, commit=False)
        return out[0][0]

    def set_stat_id(self, stat_id):
        """
        Set the stat_id of the child process
        :param stat_id:
        :return:
        """
        cmd = f'UPDATE sim SET stat_id={stat_id} WHERE id={self.sim_id}'
        self.db_exec(cmd, fetch=False, commit=True)

    def run(self):

        t = time.time()
        while self.timeout*(time.time()-t) < self.timeout**2:
            pstat_id = self.get_parent_stat_id(self.parent_id)
            if pstat_id in (0, 5):
                self.set_stat_id(5)  # Set depend_failed
                break
            if pstat_id == 3:  # Parent Completed
                self.set_stat_id(1)  # Set submitted
                break
            time.sleep(self.interval)
        # Send signal to Head Worker to garbage collect and exit
        self.queue.put(self.sim_id)
        return
