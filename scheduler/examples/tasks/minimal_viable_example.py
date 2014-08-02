import ujson


def main(textFile, ns, **job_id_identifiers):
    return ujson.dumps(textFile)


# And don't forget you would need to:

# 1. add this to the tasks graph

#   "test_scheduler/test_minimal": {
#     "depends_on": [],
#     "job_type": "pyspark",
#     "pymodule": "scheduler.examples.minimal_viable_example"
#   }

# 2. submit a job_id to it

#   ./bin/submit_task -a test_scheduler/test_minimal --job_id 20140501_1_test

# 3. run this app

#   python -m scheduler.runner --zookeeper_hosts localhost:2181
#     -a test_scheduler/test_minimal --write_fp /tmp/alex --read_fp ./README.md
#     --map
#
#   -- or --
#
#   python -m scheduler.runner --zookeeper_hosts localhost:2181
#     -a test_scheduler/test_minimal --write_fp /tmp/alex --read_fp ./README.md
#     --textFile
