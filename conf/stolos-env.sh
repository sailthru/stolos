# This configuration file defines the environment variables required for
# Stolos to run.  This file should be sourced just before running Stolos
# tests or examples:   $ env $(cat <this file>)

STOLOS_JOB_ID_DEFAULT_TEMPLATE={date}_{client_id}_{collection_name}
STOLOS_JOB_ID_VALIDATIONS=stolos.examples.job_id_validations

#
# Queue Backend:
#

# Redis queue backend
STOLOS_QUEUE_BACKEND=redis
STOLOS_QB_REDIS_DB=0
STOLOS_QB_REDIS_HOST=localhost
STOLOS_QB_REDIS_PORT=6379
STOLOS_QB_REDIS_SOCKET_TIMEOUT=3
STOLOS_QB_REDIS_LOCK_TIMEOUT=5
STOLOS_QB_REDIS_MAX_NETWORK_DELAY=4

# Zookeeper queue backend
# STOLOS_QUEUE_BACKEND=zookeeper
STOLOS_QB_ZOOKEEPER_HOSTS=localhost:2181
STOLOS_QB_ZOOKEEPER_TIMEOUT=5


# You can define your own custom queue backend
# STOLOS_QUEUE_BACKEND=mymodule.myqueue_backend


#
# Configuration backend.  the default is json
#

# JSON configuration backend
# STOLOS_CONFIGURATION_BACKEND=json  # enabled by default
STOLOS_TASKS_JSON=./stolos/examples/tasks.json

# Redis configuration backend
# STOLOS_CONFIGURATION_BACKEND=redis
# STOLOS_REDIS_DB=0
# STOLOS_REDIS_PORT=6379
# STOLOS_REDIS_HOST=localhost

# You can define your own custom configuration backend
# STOLOS_CONFIGURATION_BACKEND=mymodule.myconfiguration_backend
