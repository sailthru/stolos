Summary:
==============

This task scheduler manages the execution of tasks where some
tasks that may depend on other tasks.  It has the following features:

  - very simple to use (let us know if it isn't!)
  - simultaneous and fault tolerant execution of tasks (via ZooKeeper)
  - characterize dependencies in a directed acyclic multi-graph
  - support for subtasks and dependencies on arbitrary subsets of subtasks
  - language agnostic
  - "at least once" semantics (a guarantee that a task will successfully
    complete or fail after n retries)
  - designed for tasks of various sizes: from large hadoop tasks to
    tasks that take a second to complete

What this is project not:
  - not meant for "real-time" computation
  - unaware of machines, nodes, network topologies and infrastructure
  - does not (and should not) auto-scale workers
  - by itself, it does no actual "work" other than tracking job state
    and queueing future work.


Requirements:
  - ZooKeeper
  - Some Python libraries (Kazoo, Networkx, Argparse, ...)

Optional requirements:
  - Apache Spark
  - GraphViz


Background: Inspiration
==============


The inspiration for this project is that the way we manage dependencies
in our system defines how we characterize the work that exists in our
system.

Sailthru's Data Science team has a complex data pipeline and set of
requirements.  We have to build models, algorithms and data pipelines
for many clients, and this leads to a wide variety of work we have to
perform within our system.  Some work is very specific.  For instance,
we need to train the same predictive model once per (client, date).
Other tasks might be more complex: a cross-client analysis across
various groups of clients and ranges of dates.  In either case, we
cannot return results without having previously identified data sets we
need, transformed them, created some extra features on the data, and
built the model or analysis.

We may have hundreds or thousands of instances of any particular task,
and cannot sit around ensuring that work gets completed before we jump
into some kind of data analysis.  Therefore, we need a system to manage
execution of tasks.


Concept: Task Dependencies as a Directed Graph
==============


We can model dependencies between tasks as a directed graph, where nodes
are tasks and edges are dependency requirements.  The following section
explains how this scheduler uses a directed graph to define task
dependencies.

We start with an assumption that tasks depend on each other:

           Scenario 1:               Scenario 2:

              Task_A                     Task_A
                |                        /     \
                v                       v       v
              Task_B                  Task_B   Task_C
                                        |       |
                                        |      Task_D
                                        |       |
                                        v       v
                                          Task_E


In Scenario 1, Task_B cannot run until Task_A completes
In Scenario 2, Task_B and Task_C cannot run until Task_A completes, but
Task_B and Task_C can run in any order.  Also, Task_D requires Task_C
to complete, but doesn't care if Task_B has run yet.  Task_E requires Task_D
 and Task_B to have completed.

We also support the scenario where one task expands into multiple
subtasks.  The reason for this is that we run a hundred or thousand
variations of the one task, and the results of each subtask may bubble
down through the dependency graph.  

There are several ways subtasks may depend on other subtasks, and built
a system that captures them all (as far as we can tell).


Imagine the scenario where Task_A --> Task_B


            Scenario 1:

               Task_A
                 |
                 v
               Task_B


Let's say Task_A becomes multiple subtasks, Task_A_i.  And Task_B also
becomes multiple subtasks, Task_Bi.  Scenario 1 may transform into one of
the following:


###### Scenario1, Situation I

     becomes     Task_A1  Task_A2  Task_A3  Task_An
     ------->      |          |      |         |
                   +----------+------+---------+
                   |          |      |         |
                   v          v      v         v
                 Task_B1  Task_B2  Task_B3  Task_Bn

###### Scenario1, Situation II

                 Task_A1  Task_A2  Task_A3  Task_An
     or becomes    |          |      |         |
     ------->      |          |      |         |
                   v          v      v         v
                Task_B1  Task_B2  Task_B3  Task_Bn


In Situation 1, each subtask, Task_Bi, depends on completion of
all of TaskA's subtasks before it can run.  For instance, Task_B1
cannot run until all Task_A tasks (1 to n) have completed.
From the scheduler's point of view, this is not
different than the simple case where Task_A(1 to n) --> Task_Bi.  In
this case, we create a dependency graph for each Task_Bi.  See below:

###### Scenario1, Situation I (view 2)

     becomes     Task_A1  Task_A2  Task_A3  Task_An
     ------->      |          |      |         |
                   +----------+------+---------+
                                 |
                                 v
                               Task_Bi


In Situation 2, each subtask, Task_Bi, depends only on completion of its
related subtask in Task_A, or Task_Ai.  For instance, Task_B1 depends on
completion of Task_A1, but it doesn't have any dependency on Task_A2's
completion.  In this case, we create n dependency graphs.

As we have just seen, dependencies can be modeled as directed acyclic
multi-graphs.  (acyclic means no cycles - ie no loops.  multi-graph
means a it contains many separate graphs).


Concept: Job IDs
==============

The scheduler recognizes tasks (ie TaskA or TaskB) and subtasks
(TaskA_1, TaskA_2, ...).  A `job_id` identifies subtasks, and it is
made up of "identifiers" that we mash together into a job_id template.

Some example job_ids and their corresponding template might look like the
example below:

  "20140614_client1_dataset1"  <------>  "{date}_{client_id}_{dataset}"
  "20140601_analysis1"  <------>  "{date}_{your_custom_identifier}"


A job_id represents the smallest piece of work the scheduler can
recognize, and good choices in job_id structure identify how work is
changing from task to task.  For instance, assume the second job_id
(above), 20140601_analysis1, depends on all job_ids from 20140601 that
matched a specific subset clients and datasets.  We chose to identify the 
subset of clients and datasets with the name "analysis1."  But our
job_id also includes a date because we wish to run analysis1 on
different days.

Here's some general advice for choosing a job_id template:

  - What results do this task generate?  The words that differentiate
    those results are great candidates for a identifers in a job_id.
  - What parameters does this task expect?  The command-line arguments
    to a piece of code can make great job_id identiers.
  - How many different variations of this task exist?
  - How do I expect to use this task in my system?
  - How complex is my data pipeline?  Do I have any branches in my
    dependency tree?  If you have a very simple pipeline, you may simply wish
    to have all job_ids be the same across subtasks.



# TODO: continyue the readme and decide on sections
###### Assumptions

In this system, we assume PIDs just exist, and that they have the following
characteristics:

  - Each PID in redis must be in exactly 1 state at a time.  Possible
    states are: "**pending**", "**running**", "**completed**", "**failed**"
  - When a task expands into multiple PIDs, all of those PIDs are set in
    one atomic transaction
  - If a just-executed task does not set PID(s) within a predefined amount
    of time, it will be rescheduled up to a (predefined) max number of
    times and then fail.

We also assume 2 functions exist from the PID management library:

  - The **parse_pid**(...) function must extract the task_name and subtask_id for 
    each PID it receives.  If a PID doesn't have subtasks, its subtask_id
    should be the same across different executions of the task.  For
    instance, the subtask_id could always be 'None' or '1'.  The
    parse_pid function returns a dict like:


    {'task_name': 'XXX', 'subtask_id': 'YYY', ...}


  - The **get_pids**(...) function must return PIDs grouped by state,
    where each group is a set:

    [pending, running, completed, failed]


What do I need to do to use this?
==============

To use this code, you must have a few things:

1. Task Config
2. PID management
3. A method that creates client connection to a Redis server

###### Task Config
A task configuration is the mapping of tasks available to execute, what
it's dependencies are, and a few other options.  If you'd like to create
your own task config, please refer to the example included in this code
base for details.

###### PID management
When a task executes, something needs to write the PID somewhere.  See
the "Concept: PIDs and State" section for details of requirements.  We
may create a PID management tool to accompany this scheduler tool.  # TODO


Assumptions:
==============

  - See "Assumptions" section of "Concept: PIDs and State"
  - You have at least 1 cpu to execute code.  As long as tasks create
    pids, executed tasks can run anywhere, in any order, and on any system.
    The scheduler simply recognizes tasks as strings to execute in a (bash)
    shell.  Therefore, different tasks can run in hadoop cluster(s), 
    IPython Parallel, your local machine, or anywhere!
  - You can divide a tree of tasks across different queues, and you
    can also assign each tree seperately to different queues.
  - This scheduler identifies whether tasks are completed or failed by querying
    the state of various PIDs that uniquely identify each task or subtask.
