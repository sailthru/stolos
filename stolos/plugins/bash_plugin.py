from argparse import REMAINDER
from os import kill
from signal import alarm, signal, SIGALRM, SIGKILL
from subprocess import PIPE, Popen
import sys

from stolos.plugins import at, api, log_and_raise, log


def run(args, cwd=None, shell=False, kill_tree=True, timeout=-1, env=None,
        stdout=PIPE, stderr=PIPE):
    '''
    Run a command with a timeout after which it will be forcibly
    killed.

    # this is code by Alex Martelli
    http://stackoverflow.com/
    questions/1191374/subprocess-with-timeout/#answer-3326559
    '''
    class Alarm(Exception):
        pass

    def alarm_handler(signum, frame):
        raise Alarm

    p = Popen(
        args, shell=shell, cwd=cwd, stdout=stdout, stderr=stderr, env=env)
    if timeout != -1:
        signal(SIGALRM, alarm_handler)
        alarm(timeout)
    try:
        stdout, stderr = p.communicate()
        if timeout != -1:
            alarm(0)
    except Alarm:
        pids = [p.pid]
        if kill_tree:
            pids.extend(get_process_children(p.pid))
        for pid in pids:
            # process might have died before getting to this line
            # so wrap to avoid OSError: no such process
            try:
                kill(pid, SIGKILL)
            except OSError:
                pass
        return -9, '', ''
    return p.returncode, stdout, stderr


def get_process_children(pid):
    p = Popen('ps --no-headers -o pid --ppid %d' % pid, shell=True,
              stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    return [int(sp) for sp in stdout.split()]


def get_bash_cmd(app_name):
    """Lookup the bash command-line options for a bash task
    If they don't exist, return empty string"""
    dg = api.get_tasks_config()
    meta = dg[app_name]
    job_type = meta.get('job_type', 'bash')
    try:
        assert job_type == 'bash'
    except AssertionError:
        log.error(
            "App is not a bash job", extra=dict(
                app_name=app_name, job_type=job_type))
    rv = meta.get('bash_cmd', '')
    if not isinstance(rv, (str, unicode)):
        log_and_raise(
            "App config for bash plugin is misconfigured:"
            " bash_cmd is not a string", dict(app_name=app_name))
    return rv


def main(ns):
    """
    A generic plugin that schedules arbitrary bash jobs using Stolos

    Assume code is written in Python.  For Scala or R code, use another option.
    """
    job_id = ns.job_id
    ld = dict(**ns.__dict__)
    ld.update(job_id=job_id)
    log.info('Running bash job', extra=dict(**ld))
    cmd = get_bash_cmd(ns.app_name)
    if ns.bash_cmd:
        cmd += ' '.join(ns.bash_cmd)
        log.debug(
            "Appending user-supplied bash options to defaults", extra=dict(
                app_name=ns.app_name, job_id=job_id, cmd=cmd))
    if not cmd:
        raise UserWarning(
            "You need to specify bash options or configure default bash"
            " options")

    _cmdargs = dict(**ns.__dict__)
    _cmdargs.update(api.parse_job_id(ns.app_name, job_id))
    cmd = cmd.format(**_cmdargs)

    if ns.redirect_to_stderr:
        _std = sys.stderr
    else:
        _std = PIPE

    log.info('running command', extra=dict(cmd=cmd))
    returncode, stdout, stderr = run(
        cmd, shell=True, timeout=ns.watch, stdout=_std, stderr=_std)
    ld = dict(bash_returncode=returncode, stdout=stdout, stderr=stderr, **ld)
    if returncode == -9:
        log_and_raise("Bash job timed out", ld)
    elif returncode != 0:
        # this raises an error and logs output:
        log_and_raise("Bash job failed", ld)
    else:
        log.info("Bash job succeeded", extra=ld)


build_arg_parser = at.build_arg_parser([at.group(
    'Bash Job Options',
    at.add_argument(
        '--bash_cmd', nargs=REMAINDER, help=(
            "All remaining args are passed to the bash script. ie: "
            " myscript.sh arg1 --arg2 -c=4"
        )),
    at.add_argument(
        '--watch', type=int, default=-1, help=(
            "Initiate a watchdog that will kill the process"
            " after a given seconds"
        )),
    at.add_argument(
        '--redirect_to_stderr', action='store_true', help=(
            "Rather than capturing output and logging it,"
            " send output directly to sys.stderr")),
)])
