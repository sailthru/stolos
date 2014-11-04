# #!/usr/bin/env bash
#
# This example demonstrates how to incorporate your
# bash application with Stolos's bash plugin

# To run arbitrary bash code from Stolos, you need to create a
# configuration entry for it:
#
#   "test_stolos/test_bash": {
#     "job_type": "bash",
#     "bash_opts": "echo Hello from {app_name}. Completing work for {job_id}."
#   }

#
# Then, to run it via Stolos, you should queue a job in the task queue
# and then run the job

#   ./bin/stolos-submit -a test_stolos/test_bash --job_id 20140501_1_test
#
#   python -m stolos --zookeeper_hosts localhost:2181
#     -a test_stolos/test_bash


# To test out your script, you can always run your code directly in a shell.
#  However, you may prefer to test run your code using Stolos's
# bash plugin.  This is useful if you wish to verify that Stolos will
# call your application with the correct parameters.
#
#   python -m stolos --zookeeper_hosts localhost:2181
#     -a test_stolos/test_bash --bypass_scheduler --job_id 20140501_1_test
