DIR="$( dirname "$( cd "$( dirname "$0" )" && pwd )")"

. $DIR/conf/stolos-env.sh
echo $STOLOS_ZOOKEEPER_HOSTS
echo $STOLOS_TASKS_JSON
echo $STOLOS_JOB_ID_DEFAULT_TEMPLATE
echo $STOLOS_JOB_ID_VALIDATIONS
echo $STOLOS_CONFIGURATION_BACKEND

echo -n Is a local Zookeeper Server running and available?
ans=`pgrep -f '\-Dzookeeper' >/dev/null && echo yes || echo no`
echo ...$ans
echo -n Is a local Redis Server running and available?
ans2=`pgrep -f 'redis-server' && echo yes || echo no`
echo ...$ans2

if [ "$ans" = "no" -o "$ans2" = "no" ] ; then
  echo \\n
  echo CANNOT RUN TESTS!\\n
  echo Tests require that you have a Zookeeper and Redis server
  exit 1
fi

python $DIR/bin/code_linter.py || exit 1

nosetests $@
