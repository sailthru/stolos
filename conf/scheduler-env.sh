#!/usr/bin/env bash

if [ -z "$DIR" ] ; then DIR='.' ; fi
export TASKS_JSON="$DIR/scheduler/examples/tasks.json"
export JOB_ID_DEFAULT_TEMPLATE="{date}_{client_id}_{collection_name}"
export JOB_ID_VALIDATIONS="scheduler.examples.job_id_validations"
