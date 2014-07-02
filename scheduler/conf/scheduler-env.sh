#!/usr/bin/env bash

DIR="$( cd "$( dirname "$0" )" &>/dev/null && pwd )"

export TASKS_JSON="$DIR/scheduler/examples/tasks.json"
export JOB_ID_DEFAULT_TEMPLATE="{date}_{client_id}_{collection_name}"
export JOB_ID_VALIDATIONS="scheduler.examples.job_id_validations"
