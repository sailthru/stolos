#!/usr/bin/env bash

# This configuration file defines the environment variables required for
# Stolos to run.  This file should be sourced just before running Stolos
# tests or examples:   $ source <this file>

if [ -z "$DIR" ] ; then DIR='.' ; fi
export STOLOS_JOB_ID_DEFAULT_TEMPLATE="{date}_{client_id}_{collection_name}"
export STOLOS_JOB_ID_VALIDATIONS="stolos.examples.job_id_validations"

# you can specify a different configuration backend.  the default is json
unset STOLOS_CONFIGURATION_BACKEND

# JSON configuration backend
# export STOLOS_CONFIGURATION_BACKEND="stolos.configuration_backend.json_config.JSONConfig"
export STOLOS_TASKS_JSON="$DIR/stolos/examples/tasks.json"

# Zookeeper configuration backend
export STOLOS_ZOOKEEPER_HOSTS="localhost:2181"


# Redis configuration backend
# export STOLOS_CONFIGURATION_BACKEND="stolos.configuration_backend.redis_config.RedisMapping"
# export STOLOS_REDIS_DB=3
# export STOLOS_REDIS_PORT=6379
# export STOLOS_REDIS_HOST='localhost'
