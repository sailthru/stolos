from stolos.initializer import initialize as _initialize
from stolos import dag_tools as _dt
from stolos import configuration_backend as _cb
from stolos import queue_backend as _qb

from stolos.queue_backend import (
    check_state, maybe_add_subtask, readd_subtask, get_qbclient
)
# linting
check_state, maybe_add_subtask, readd_subtask, get_qbclient

from stolos.dag_tools import (
    build_dag, visualize_dag, topological_sort,
    create_job_id, parse_job_id, get_job_id_template,
    get_parents, get_children,
)
build_dag, visualize_dag, topological_sort
create_job_id, parse_job_id, get_job_id_template,
get_parents, get_children

from stolos.configuration_backend import get_tasks_config
get_tasks_config,

from stolos.util import configure_logging
configure_logging  # can be used to modify how stolos logs things.


def initialize(args=None):
    """
    Initialize Stolos.  This function must be called before Stolos's api is
    usable.  It fetches all required configuration variables to use stolos's
    library.  Will not load namespace options required by Stolos plugins.

    `args` - (optional).  Define command-line arguments to use.
        Default to sys.argv (which is what argparse does).
        Explicitly pass args=[] to not read command-line arguments, and instead
        expect that all arguments are passed in as environment variables.
        To guarantee NO arguments are read from sys.argv, set args=[]
        Example:  args=['--option1', 'val', ...]
    """
    _initialize([_dt, _cb, _qb], args=args)


def get_qsize(app_name, queued=True, taken=True):
    """Get the number of objects in the queue"""
    return get_qbclient().LockingQueue(app_name).size(
        queued=queued, taken=taken)
