DIR="$( dirname "$( cd "$( dirname "$0" )" && pwd )")"

. $DIR/conf/scheduler-env.sh

nosetests \
`python -c '
from os.path import dirname ;
import scheduler ;
print(dirname(scheduler.__file__))'
`/tests $@
