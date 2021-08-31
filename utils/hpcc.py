import time
import subprocess
from subprocess import PIPE

# Mapping scheduler specific status codes to db status codes (stat_id, combination_rule, codes)
# The database stat_ids code correspond to: 0: failed, 2: running 3: complete
# The combination rule is applied to aggregate jobs (multiple jobIDs)
# Thus if one job in the array failed the set will be flagged as failed independent of the other jobs
# The Status_codes are sorted by priority
# i.e. if one job failed and 3 are pending the aggregate job will be flagged failed

STATUS_CODES = {'Slurm': [(0, any, ('FAILED', 'PREEMPTED', 'SUSPENDED', 'STOPPED')),
                          (2, any, ('RUNNING', 'COMPLETING', 'PENDING')),
                          (3, all, ('COMPLETED', ))],
                'SGE': [(0, any, ('f', )),
                        (2, any, ('r', )),
                        (3, all, ('c', ))]}


class JobStatus(object):
    """
    A simple JobMonitor
    """
    def __init__(self, job_ids, scheduler=None):

        # If a single job_id is passed
        if isinstance(job_ids, int):
            self.job_ids = [job_ids, ]
        elif is_iterable(job_ids):
            self.job_ids = job_ids
        else:
            raise ValueError(f'JobIDs must be int or iterable but got {type(job_ids)}')

        if scheduler is None:
            scheduler = get_scheduler()

        if scheduler in ('LSF', ):
            raise NotImplementedError('gmx_db only supports Slurm & SGE Scheduler currently')
        elif scheduler not in ('Slurm', 'SGE'):
            raise ValueError(f'unknown scheduler: {scheduler}')
        else:  # Get the appropriate query function
            self.query = {'Slurm': slurm_job_status,
                          'SGE': sge_job_status}[scheduler]

        # Get the status codes for the corresponding scheduler
        self.status_codes = STATUS_CODES[scheduler]
        # Set initial _job_status
        self._job_status = None
        self.status_query()

    @property
    def status(self):
        return self._job_status

    @status.getter
    def status(self):
        self.status_query()
        return self._job_status

    def status_query(self):
        """
        Return the combined job status
        See comments on the global: STATUS_CODES for a indepth description
        :return:
        """
        job_status = []
        for jid in self.job_ids:
            job_status.append(self.query(jid))

        for stat_id, combination_func, stat_codes in self.status_codes:
            if combination_func([s in stat_codes for s in job_status]):
                self._job_status = stat_id
                break


def is_iterable(obj):
    """
    Return true if an object is iterable
    :param obj:
    :return:
    """
    try:
        _ = iter(obj)
        return True
    except:
        return False


def get_scheduler():
    """
    Try to determine the scheduler
    :return:
    """
    queue_info = {'Slurm': 'sinfo',
                  'SGE': 'qstat',
                  'LSF': 'bqueue'}

    for name, cmd in queue_info.items():
        out = subprocess.run(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        if out.returncode:
            continue
        else:
            return name
    return


def slurm_job_status(job_id, retries=5, interval=10):
    """
    Get job status with slurm
    :param job_id:
    :param retries:
    :param interval
    :return:
    """
    cmd = f'sacct -j {job_id} --delimiter=\',\' --parsable2 --format=JobID,State,ExitCode'
    for i in range(retries):
        out = subprocess.run(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        if out.returncode:
            raise RuntimeError(f'Non-zeros exitcode: {out.returncode}\n{out.stdout}\n{out.stderr}')
        else:
            lines = out.stdout.decode('UTF-8').split('\n')
            try:
                job_id, status, exitcode = lines[1].split(',')
                return status
            except Exception as e:
                print(f'Failed to get job status for {job_id}')
                time.sleep(interval)
    raise ValueError(f'Unexpected output for job_id: {job_id}\n{lines}')


def sge_job_status(job_id, retries=5, interval=10):
    """
    Get job status with sge
    On SGE we need to call two separate commands for running and finished jobs
    qstat will return all jobs that are active
    qacct will also show jobs that have finished
    :param job_id:
    :param retries:
    :param interval:
    :return:
    """
    # List of "active states" https://gist.github.com/cmaureir/4fa2d34bc9a1bd194af1
    active_states = ('qw', 'hqw', 'hRwq', 'r', 't', 'Rr', 'Rt', 's', 'ts', 'S', 'tS')
    for _ in range(retries):
        is_active = False  # Not active until proofen otherwise
        # Get all active jobs
        out = subprocess.run('qstat', shell=True, stdout=PIPE, stderr=PIPE)
        if out.returncode:  # Just wait a little bit and try again
            time.sleep(interval)
            continue

        else:
            lines = out.stdout.decode('UTF-8').split('\n')
            if len(lines) >= 3:  # If there are any active jobs qstat returns a two line header and the job info
                for line in lines[2:]:  # The first two lines are header
                    line_split = line.split()
                    if len(line_split) > 1:
                        jid = line_split[0]
                        state = line_split[4]
                    else:
                        continue
                    if jid == str(job_id) and state in active_states:
                        is_active = True
                        break
        if is_active:
            return 'r'
        else:
            cmd = f'qacct -j {job_id}'
            out = subprocess.run(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            if out.returncode:  # Just wait a little bit and try again
                time.sleep(interval)
                continue

            else:
                lines = out.stdout.decode('UTF-8').split('\n')
                for line in lines[1:]:  # Skip Header
                    line_split = line.split()
                    if len(line_split) == 2:
                        kw, val = line.split()
                        if kw == 'exit_status':
                            if val == '0':
                                return 'c'
                            else:
                                return 'f'
                    else:
                        continue
        raise RuntimeError(f'Non-zeros exitcode: {out.returncode}\n{out.stdout}\n{out.stderr}')
