#!/usr/bin/env bash

# This configuration file defines the environment variables required for
# scheduler to run.  This file should be sourced just before running scheduler
# tests or examples:   $ source <this file>

if [ -z "$DIR" ] ; then DIR='.' ; fi
export JOB_ID_DEFAULT_TEMPLATE="{date}_{client_id}_{collection_name}"
export JOB_ID_VALIDATIONS="scheduler.examples.job_id_validations"

# you can specify a different configuration backend.  the default is json
export TASKS_JSON="$DIR/scheduler/examples/tasks.json"
unset CONFIGURATION_BACKEND
# export CONFIGURATION_BACKEND="scheduler.configuration_backend.json_config.JSONConfig"
