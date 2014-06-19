export TASKS_JSON="$HOME/ds/scheduler/examples/tasks.json"
export JOB_ID_DEFAULT_TEMPLATE="{date}_{client_id}_{collection_name}"
export JOB_ID_VALIDATIONS="tasks.job_id_validations"

nosetests \
`python -c '
from os.path import dirname ;
import scheduler ;
print(dirname(scheduler.__file__))'
`/tests
