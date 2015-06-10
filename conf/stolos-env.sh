#!/usr/bin/env bash

# This configuration file defines the environment variables required for
# Stolos to run.  This file should be sourced just before running Stolos
# tests or examples:   $ source <this file>

if [ -z "$DIR" ] ; then DIR='.' ; fi
export STOLOS_JOB_ID_DEFAULT_TEMPLATE="{date}_{client_id}_{collection_name}"
export STOLOS_JOB_ID_VALIDATIONS="stolos.examples.job_id_validations"

#
# Queue Backend:
#

export STOLOS_QUEUE_BACKEND="zookeeper"  # enabled by default
export STOLOS_QB_ZOOKEEPER_HOSTS="localhost:2181"
export STOLOS_QB_ZOOKEEPER_TIMEOUT=5

# Redis queue backend
# export STOLOS_QUEUE_BACKEND="majorityredis"
export STOLOS_QB_REDIS_DB=0
export STOLOS_QB_REDIS_HOSTS="localhost:6379"
export STOLOS_QB_REDIS_N_SERVERS=1
export STOLOS_QB_REDIS_SOCKET_TIMEOUT="5"
export STOLOS_QB_REDIS_HISTORY_PREFIX="test_stolos"

# You can define your own custom configuration backend
# export STOLOS_QUEUE_BACKEND="mymodule.myqueue_backend"


#
# Configuration backend.  the default is json
#

# JSON configuration backend
# export STOLOS_CONFIGURATION_BACKEND="json"  # enabled by default
export STOLOS_TASKS_JSON="$DIR/stolos/examples/tasks.json"

# Redis configuration backend
# export STOLOS_CONFIGURATION_BACKEND="redis"
# export STOLOS_REDIS_DB=0
# export STOLOS_REDIS_PORT=6379
# export STOLOS_REDIS_HOST='localhost'

# You can define your own custom configuration backend
# export STOLOS_CONFIGURATION_BACKEND="mymodule.myconfiguration_backend"
