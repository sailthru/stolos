DIR="$( dirname "$( cd "$( dirname "$0" )" && pwd )")"

. $DIR/conf/stolos-env.sh
echo $TASKS_JSON
echo $JOB_ID_DEFAULT_TEMPLATE
echo $JOB_ID_VALIDATIONS
echo $CONFIGURATION_BACKEND

python $DIR/bin/code_linter.py || exit 1

nosetests \
`python -c '
from os.path import dirname ;
import stolos ;
print(dirname(stolos.__file__))'
`/tests $@
