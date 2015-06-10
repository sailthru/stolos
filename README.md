
Summary:
==============

Stolos is a task dependency scheduler that helps build distributed pipelines.
It shares similarities with [Chronos](https://github.com/mesos/chronos),
[Luigi](http://luigi.readthedocs.org/en/latest/), and
[Azkaban](http://azkaban.github.io/azkaban/docs/2.5/), yet remains
fundamentally different from all three.

The goals of Stolos are the following:

  - Manage the order of execution of interdependent applications, where each
    application may run many times with different input parameters.
  - Provide an elegant way to define and reason about job dependencies.
  - Built for fault tolerance and scalability.
  - Applications are completely decoupled form and know nothing about Stolos.

![](/screenshot.png?raw=true)


How does Stolos work?
==============

Stolos consists of three primary components:

- a Queue (stores job state)
- a Configuration (defines task dependencies.  this is a JSON file by default)
- the Runner (ie. runs code via bash command or a plugin)

**Stolos manages a queuing system** to decide, at any given point in time, if
the current application has any jobs, if the current job is runnable, whether
to queue child or parent jobs, or whether to requeue the current job if it
failed.

**Stolos "wraps" jobs.**  This is an important concept for three reasons.
First, before the job starts and after the job finishes, Stolos updates the
job's state in the queueing system.  Second, rather than run a job directly (ie
from command-line), Stolos runs directly from the command-line, where it will
check the application's queue and run a queued job.  If no job is queued,
Stolos will wait for one or exit.  Third, Stolos must run once for every job in
the queue.  Stolos is like a queue consume, and an external process must
maintain a healthy number of queue consumers.  This can be done with crontab
(meh), [Relay.Mesos](https://github.com/sailthru/relay.mesos), or any auto
scaler program.

Stolos lets its **users define deterministic dependency
relationships between applications**.  The documentation explains this in
detail.  In the future, we may let users define non-deterministic dependency
relationships, but we don't see the benefits yet.

**Applications are completely decoupled from Stolos.** This means applications
can run independently of Stolos and can also integrate directly with it without
any changes to the application's code.  Stolos identifies an application via a
Configuration, defined in the documentation.

Unlike many other dependency schedulers, **Stolos is decentralized**.  There is
no central server that "runs" things.  Decentralization here means a few
things.  First, Stolos does not care where or how jobs run.  Second, it doesn't
care about which queuing or configuration backends are used, provided that
Stolos is able to communicate with these backends.  Third, and perhaps most
importantly, the mission-critical questions about Consistency vs Availability
vs Partition Tolerance (as defined by the CAP theorem) are delegated to the
queue backend (and to some extent, the configuration backend).



Stolos, in summary:
==============

  - manages job state, queues future work, and starts your applications.
  - language agnostic (but written in Python).
  - "at least once" semantics (a guarantee that a job will successfully
    complete or fail after n retries)
  - designed for apps of various sizes: from large hadoop jobs to
    jobs that take a second to complete

What this is project not:
--------------

  - not aware of machines, nodes, network topologies and infrastructure
  - does not (and should not) auto-scale workers
  - not (necessarily) meant for "real-time" computation
  - This is not a grid scheduler (ie this does not solve a bin packing problem)
  - not a crontab.  (in certain cases this is not entirely true)
  - not meant to manage long-running services or servers (unless order
    in which they start is important)


Similar tools out there:
--------------

  - [Chronos](https://github.com/airbnb/chronos)
  - [Luigi](http://luigi.readthedocs.org/en/latest/), and
  - [Azkaban](http://azkaban.github.io/azkaban/docs/2.5/), yet fundamentally
  - [Dagobah](https://github.com/thieman/dagobah)
  - [Quartz](http://quartz-scheduler.org/)
  - [Cascading](http://www.cascading.org/documentation/)


Requirements:
--------------

  - A Queue backend (ZooKeeper or Redis for now)
  - A Configuration backend (JSON file, Redis, ...)
  - Some Python libraries (Kazoo, Networkx, Argparse, ...)

Optional requirements:
--------------

  - Apache Spark
  - GraphViz


Background: Inspiration
==============


The inspiration for this project comes from the notion that the way we
manage dependencies in our system defines how we characterize the work
that exists in our system.

This project arose from the needs of Sailthru's Data Science team to manage
execution of pipeline applications.  The team has a complex data pipeline
(build models, algorithms and applications that support many clients), and this
leads to a wide variety of work we have to perform within our system.  Some
work is very specific.  For instance, we need to train the same predictive
model once per (client, date).  Other tasks might be more complex: a
cross-client analysis across various groups of clients and ranges of dates.  In
either case, we cannot return results without having previously identified data
sets we need, transformed them, created some extra features on the data, and
built the model or analysis.

Since we have hundreds or thousands of instances of any particular application,
we cannot afford to manually verify that work gets completed. Therefore,
we need a system to manage execution of applications.


Concept: Application Dependencies as a Directed Graph
==============


We can model dependencies between applications as a directed graph, where nodes
are apps and edges are dependency requirements.  The following section
explains how Stolos uses a directed graph to define application
dependencies.

We start with an assumption that our applications depend on each other:

           Scenario 1:               Scenario 2:

              App_A                     App_A
                |                        /     \
                v                       v       v
              App_B                   App_B    App_C
                                        |       |
                                        |      App_D
                                        |       |
                                        v       v
                                          App_E


In Scenario 1, `App_B` cannot run until `App_A` completes.  In
Scenario 2, `App_B` and `App_C` cannot run until `App_A` completes,
but `App_B` and `App_C` can run in any order.  Also, `App_D` requires
`App_C` to complete, but doesn't care if `App_B` has run yet.
`App_E` requires `App_D` and `App_B` to have completed.

By design, we also support the scenario where one application expands into
multiple subtasks, or jobs.  The reason for this is that if we run a hundred or
thousand variations of the one app, the results of each job (ie subtask) may
bubble down through the dependency graph independently of other jobs.

There are several ways jobs may depend on other jobs, and this
system captures all deterministic dependency relationships (as far as we can
tell).


Imagine the scenario where `App_A` --> `App_B`


            Scenario 1:

               App_A
                 |
                 v
               App_B


Let's say `App_A` becomes multiple jobs, or subtasks, `App_A_i`.  And `App_B`
also becomes multiple jobs, `App_Bi`.  Scenario 1 may transform
into one of the following:


###### Scenario1, Situation I

     becomes     App_A1  App_A2  App_A3  App_An
     ------->      |          |      |         |
                   +----------+------+---------+
                   |          |      |         |
                   v          v      v         v
                 App_B1  App_B2  App_B3  App_Bn

###### Scenario1, Situation II

                 App_A1  App_A2  App_A3  App_An
     or becomes    |          |      |         |
     ------->      |          |      |         |
                   v          v      v         v
                App_B1  App_B2  App_B3  App_Bn


In Situation 1, each job, `App_Bi`, depends on completion of all of `App_A`'s
jobs before it can run.  For instance, `App_B1` cannot run until all `App_A`
jobs (1 to n) have completed.  From Stolos's point of view, this is not
different than the simple case where `App_A(1 to n)` --> `App_Bi`.  In this
case, we create a dependency graph for each `App_Bi`.  See below:

###### Scenario1, Situation I (view 2)

     becomes     App_A1  App_A2  App_A3  App_An
     ------->      |          |      |         |
                   +----------+------+---------+
                                 |
                                 v
                               App_Bi


In Situation 2, each job, `App_Bi`, depends only on completion of
its related job in `App_A`, or `App_Ai`.  For instance,
`App_B1` depends on completion of `App_A1`, but it doesn't have any
dependency on `App_A2`'s completion.  In this case, we create n
dependency graphs, as shown in Scenario 1, Situation II.

As we have just seen, dependencies can be modeled as directed acyclic
multi-graphs.  (acyclic means no cycles - ie no loops.  multi-graph
contains many separate graphs).  Situation 2 is the
default in Stolos (`App_Bi` depends only on `App_Ai`).


Concept: Job IDs
==============

*For details on how to use and configure `job_id`s, see the section, [Job
ID Configuration](/README.md#configuration-job-ids)*  This section
explains what `job_id`s are.

Stolos recognizes apps (ie `App_A` or `App_B`) and jobs (`App_A1`,
`App_A2`, ...).  An application, or app, represents a group of jobs.  A
`job_id` identifies jobs, and it is made up of "identifiers" that we mash
together via a `job_id` template.  A `job_id` identifies all possible
variations of some application that Stolos is aware of.  To give some
context for how `job_id` templates characterize apps, see below:


               App_A    "{date}_{client_id}_{dataset}"
                 |
                 v
               App_B    "{date}_{your_custom_identifier}"


Some example `job_id`s of `App_A` and `App_B`, respectively, might be:

    App_A:  "20140614_client1_dataset1"  <--->  "{date}_{client_id}_{dataset}"
    App_B:  "20140601_analysis1"  <--->  "{date}_{your_custom_identifier}"


A `job_id` represents the smallest piece of work that Stolos can
recognize, and good choices in `job_id` structure identify how work is
changing from app to app.  For instance, assume the second `job_id`
above, `20140601_analysis1`, depends on all `job_id`s from `20140601`
that matched a specific subset of clients and datasets.  We chose to
identify this subset of clients and datasets with the name `analysis1`.
But our `job_id` template also includes a `date` because we wish to run
`analysis1` on different days.  Note how the choice of `job_id`
clarifies what the first and second apps have in common.

Here's some general advice for choosing a `job_id` template:

  - What results does this app generate?  The words that differentiate this
    app's results from other apps' results are great candidates for identifers
    in a `job_id`.
  - What parameters does this app expect?  The command-line arguments
    to a piece of code can be great `job_id` identiers.
  - How many different variations of this app exist?
  - How do I expect to use this app in my system?
  - How complex is my data pipeline?  Do I have any branches in my
    dependency tree?  If you have a very simple pipeline, you may simply
    wish to have all `job_id` templates be the same across apps.

It is important to note that the way(s) in which `App_B` depends on
`App_A` have not been explained in this section.  A `job_id` does not
explain how apps depend on each other, but rather, it characterizes how
we choose to identify a app's jobs in context of the parent and child
apps.


Concept: Bubble Up and Bubble Down
==============

"Bubble Up" and "Bubble Down" refer to the direction in which work and
app state move through the dependency graph.

Recall Scenario 1, which defines two apps.  `App_B` depends on
`App_A`.  The following picture is a dependency tree:

           Scenario 1:

              App_A
                |
                v
              App_B

"Bubble Down"
--------------

By analogy, the "Bubble Down" approach is like "pushing" work through a
pipe.

Assume that `App_A` and `App_B` each had their own `job_id` queue.  A
`job_id`, `job_id_123` is submitted to `App_A`'s queue, some worker
fetches that job, completes required work, and then marks the
`(App_A, job_id_123)` pair as completed.

The "Bubble Down" process happens when, just before `(App_A,
job_id_123)` is marked complete, we queue `(App_B, f(job_id_123))`
where `f()` is a magic function that translates `App_A`'s `job_id` to
the equivalent `job_id` for `App_B`.

In other words, the completion of `App_A` work triggers the completion
of `App_B` work.  A more semantically correct version is the following:
the completion of `(App_A, job_id_123)` depends on both the successful
execution of `App_A` code and then successfully queuing some `App_B` work.


"Bubble Up"
--------------

The "Bubble Up" approach is the concept of "pulling" work through a
pipe.

In contrast to "Bubble Down", where we executed `App_A` first, "Bubble
Up" executes `App_B` first.  "Bubble Up" is a process of starting at
some child (or descendant job), queuing the furthest uncompleted and
unqueued ancestor, and removing the child from the queue.  When
ancestors complete, they will queue their children via "Bubble Down" and
re-queue the original child job.

For instance, we can attempt to execute `(App_B, job_id_B)` first.
When `(App_B, job_id_B)` runs, it checks to see if its parent,
`(App_A, g(job_id_B))` has completed.  Since `(App_A, g(job_id_B))`
has not completed, it queues this job and then removes `job_id_B` from
the `App_B` queue.  Finally, `App_A` executes and via "Bubble Down",
`App_B` also completes.


Magic functions f() and g()
--------------

Note that `g()`, mentioned in the "Bubble Up" subsection, is the inverse
of `f()`, mentioned in the "Bubble Down" subsection.  If `f()` is a magic
function that translates `App_A`'s `job_id` to `App_B`'s `job_id`,
then `g()` is a similar magic function that transforms a `App_B` `job_id`
to an equivalent one for `App_A`.  In reality, `g()` and `f()` receive one
`job_id` as input and return at least one `job_id` as output.

These two functions can be quite complex:

  - If the parent and child app have the same `job_id` template, then `f()
    == g()`.  In other words, `f()` and `g()` return the same `job_id`.
  - If they have different templates, the functions will attempt to use
    the metadata available from configuration metadata (ie in TASKS_JSON)
  - If a parent has many children, `f(parent_job_id)` returns a `job_id`
    for each child and `g(child_id)` returns at least 1 `job_id` for
    that parent app.  This may involve calculating the crossproduct of
    `job_id` identifier metadata listed in dependency configuration for
    that app.
    - If a child has many parents, `g` and `f` perform similar
      operations.


Why perform a "Bubble Up" operation at all?
--------------

If Stolos was purely a "Bubble Down" system (like many task dependency
schedulers), executing `App_B` first means we
would have to wait indefinitely until `App_A` successfully completed
and submitted a `job_id` to the queue.  This can pose many problems: we
don't know if `App_A` will ever run, so we sit and wait; waiting
processes take up resources and become non-deterministic (we have no
idea if the process will hang indefinitely); we can create locking
scenarios where there aren't enough resources to execute `App_A`;
`App_B`'s queue size can become excessively high; we suddenly need a
queue prioritization scheme and other complex algorithms to manage
scaling and resource contention.

If a task dependency system, such as Stolos, supports a "Bubble Up" approach,
we can simply pick and run any app in a dependency graph and expect that it
will be queued to execute as soon as possible to do so.  This avoids the above
mentioned problems.

Additionally, if Stolos is used properly, "Bubble up" will never queue
particular jobs that would otherwise be ignored.


Do I need to choose between "Bubble Up" and "Bubble Down" modes?
--------------

Stolos performs "Bubble Up" and "Bubble Down" operations simultaneously,
and as a user, you do not need to choose whether to set Stolos into "Bubble Up"
mode or "Bubble Down" mode, as both work by default.

The key question you do need to answer is how do you want to start the jobs in
your dependency graph.  You can start by queueing jobs at the very top of a
tree and then "bubble down" to all of your child jobs.  You could also start by
queueing the last node in your tree, or you can start jobs from somewhere in
the middle.  In the latter two cases, jobs would "Bubble Up" and then "Bubble
Down" as described earlier in this section.

You can even start jobs from the top and the bottom of a tree at the same time,
though there is not much point to that.  If you have multiple dependency trees
defined in the graph, you can start some from the top (and bubble down) and
others from the bottom (and bubble up).  It really doesn't matter how you queue
jobs because whater you choose to do, Stolos will eventually run all parent and
then child jobs from your chosen starting point.


Concept: Job State
==============

There are 4 recognized job states.  A job_id should be in any one of these
states at any given time.

- `completed`  --  When a job has successfully completed the work defined by a
  `job_id`, and children have been queued, the `job_id` is marked as completed.
- `pending`  --  A `job_id` is pending when it is queued for work or otherwise
    waiting to be queued on completion of a parent job.
- `failed`  --  Failed `job_id`s have failed more than the maximum allowed
  number of times.  Children of failed jobs will never be executed.
- `skipped`  --  A `job_id` is skipped if it does not pass valid_if_or criteria
    defined for that app.  A skipped job is treated like a "failed" job.


Setup:
==============

The first thing you'll want to do is install Stolos

    pip install stolos

    # If you prefer a portable Python egg, clone the repo and then type:
    # python setup.py bdist_egg


Next, define environment vars.  You decide here which configuration backend and
queue backend you will use.  See this [sample environment
configuration](conf/stolos-env.sh) for details.

    export STOLOS_JOB_ID_DEFAULT_TEMPLATE="{date}_{client_id}_{collection_name}"
    export STOLOS_JOB_ID_VALIDATIONS="my_python_codebase.job_id_validations"

    # assuming you choose the default JSON configuration backend:
    export STOLOS_TASKS_JSON="/path/to/a/file/called/tasks.json"

    # and assuming you use the default Zookeeper queue backend:
    export STOLOS_QB_ZOOKEEPER_HOSTS="localhost:2181"


Next, tell Stolos how your applications depend on each other.  In the
environment vars defined above, we assume you're using the default backend, a
JSON file.  See the links below:


- Learn about [Job Ids](README.md#configuration-job-ids)

- Help me define app configuration: [Configuration: Apps, Dependencies and
  Configuration ](README.md#configuration-apps-dependencies-and-configuration)

Last, create a `job_id_validations` python file that should look like this:

- See [example `job_id` validations file](stolos/examples/job_id_validations.py)


[Use Stolos to run my
applications](README.md#usage)


For help configuring non-default options:
- Non-default configuration backend: [Setup: Configuration

  Backends](README.md#setup-configuration-backends)

- Non-default queue backend: [Setup: Queue
  Backends](README.md#setup-queue-backends)


Setup: Configuration Backends
==============

The configuration backend identifies where you store the dependency graph.  By
default, and in all examples, Stolos expects to use a a simple json file
to store the dependency graph.  However, you can choose to store this data in
other formats or databases.  The choice of configuration backend defines how
you store configuration.  Also, keep in mind that every time a Stolos app
initializes, it queries the configuration.

Currently, the only supported configuration backends are a JSON file or a Redis
database.  However, it is also simple to extend Stolos with your own
configuration backend.  If you do implement your own configuration backend,
please consider submitting a pull request to us!

These are the steps you need to take to use a non-default backend:

1. First, let Stolos know which backend to load.  (You could also specify
   your own configuration backend, if you are so inclined).

```
export STOLOS_CONFIGURATION_BACKEND="json"  # default
```

OR

```
export STOLOS_CONFIGURATION_BACKEND="redis"
```

OR

```
export STOLOS_CONFIGURATION_BACKEND="mymodule.mybackend.MyMapping"
```

2. Second, each backend has its own options.
    - For the JSON backend, you must define:

        export STOLOS_TASKS_JSON="$DIR/stolos/examples/tasks.json"

    - For the Redis backend, it is optional to overide these defaults:

        export STOLOS_REDIS_DB=0  # which redis db is Stolos using?
        export STOLOS_REDIS_PORT=6379
        export STOLOS_REDIS_HOST='localhost'


For examples, see the file, [conf/stolos-env.sh](conf/stolos-env.sh)


Setup: Queue Backends
==============

The queue backend identifies where (and how) you store job state.
By default, Stolos uses the Zookeeper backend.

Currently, the only supported queue backends are Zookeeper and Redis.  Both of
these databases have strong consistency guarantees, which are important if you
care about maintaining distributed locks.  If using the Redis backend with
replication, be careful to follow the Redis documentation about
[Replication](http://redis.io/topics/replication)

These are the steps you need to take to use a non-default backend:

1. First, let Stolos know which backend to load.

```
export STOLOS_QUEUE_BACKEND="zookeeper"  # default
```

OR

```
export STOLOS_QUEUE_BACKEND="redis"
```

2. Second, each backend has its own options.
    - For the Zookeeper backend, you must define:

        export STOLOS_QB_ZOOKEEPER_HOSTS="localhost:2181"  # or appropriate uri

    - For the Redis backend, it is optional to overide these defaults:

        export STOLOS_REDIS_DB=0  # which redis db is Stolos using?
        export STOLOS_REDIS_PORT=6379
        export STOLOS_REDIS_HOST='localhost'


For examples, see the file, [conf/stolos-env.sh](conf/stolos-env.sh)


Usage: Quick Start
==============

Great! You've installed and configured Stolos.  Let's run an application.

1. Create some application that can be called through bash or initiated
   as a spark job.  Let's use ```echo 123``` because it is a good test example.
1. Create an entry for it in the app config (ie the file pointed to by
   `TASKS_JSON` if you use that)

```
cat > $TASKS_JSON <<EOF
{
    "myapp": {
        "job_id": "{num}",
        "bash_cmd": "echo 123"
    }
}
EOF
```


1. Submit a `job_id` for this app

```
stolos-submit -a myapp --job_id 123
```

1. Run the app using Stolos (see below)

```
stolos -a myapp
```

**Take a look at some [examples](stolos/examples/tasks/) for examples**


Usage:
==============

Stolos wraps your application code so it can track job state just before and
just after the application runs.  Therefore, you should think of running an
application via Stolos like running the application itself.  This is
fundamentally different than many projects because there is no centralized
server or "master" node from you can launch tasks or control Stolos.  Stolos is
simply a thin wrapper for your application.  It is ignorant of your
infrastructure and network topology.  You can generally execute Stolos
applications the same way you would treat your application without Stolos.
Keep in mind, though, that if you run Stolos, your job will not run at that
moment unless it is queued.


This is how you can manually queue a job:


    stolos-submit -h


In order to run a job, you have to queue it and then execute it.  You
can get a job from the application's queue and execute code via:


    # to see demo example, run first:
    . ./conf/stolos-env.sh

    stolos -a app1 -h



We also provide a way to bypass Stolos and execute a job
directly.  This isn't useful unless you are testing an app that may be
dependent on Stolos plugins, such as the pyspark plugin.


    stolos --bypass_scheduler -a my_app --job_id my_job_id


Configuration: Job IDs
==============

This section explains what configuration for `job_id`s must exist.

These environment variables must be available to Stolos:

    export STOLOS_JOB_ID_DEFAULT_TEMPLATE="{date}_{client_id}_{collection_name}"
    export STOLOS_JOB_ID_VALIDATIONS="my_python_codebase.job_id_validations"

- `STOLOS_JOB_ID_VALIDATIONS` points to a python module containing code to
  verify that the identifiers in a `job_id` are correct.
  - See
    [stolos/examples/job_id_validations.py](stolos/examples/job_id_validations.py)
    for the expected code structure
  - These validations specify exactly which identifiers can be used in
    job_id templates and what format they take (ie is `date` a datetime
    instance, an int or string?).
  - They are optional to implement, but you will see several warning
    messages for each unvalidated `job_id` identifier.
- `STOLOS_JOB_ID_DEFAULT_TEMPLATE` - defines the default `job_id` for an app if
  the `job_id` template isn't explicitly defined in the app's config.  You
  should have `job_id` validation code for each identifier in your default
  template.

In addition to these defaults, each app in the app configuration may also
contain a custom `job_id` template.  See section: [Configuration: Apps,
Dependencies and Configuration
](README.md#configuration-apps-dependencies-and-configuration) for details.


Configuration: Apps, Dependencies and Configuration
==============

App configuration defines the app dependency graph and metadata
Stolos needs to run a job.  App configuration answers questions
like: "What are the parents or children for this (`app_name`, `job_id`)
pair?" and "What general key:value configuration is defined for this
app?".  It does not store the state of `job_id`s and it does not contain
queuing logic.  This section will show available configuration options.

Configuration can be defined using various different configuration backends:
a json file, a key-value database, etc. For instructions on how to setup a
different or custom configuration backends, see section
"Setup: Configuration Backends."  The purpose of this section, however, is to
expose how to define the apps and their relationships with other apps.

There are a few different configuration options each app may define.  Here is
a list of configuration options:

- *`job_type`* - (optional) Select which plugin this particular app uses
  to execute your code.  The default `job_type` is `bash`.
- *`depends_on`* - (optional) A designation that a (`app_name`, `job_id`)
  can only be queued to run if certain parent `job_id`s have completed.
- *`job_id`* - (optional) A template describing what identifiers compose
  the `job_id`s for your app.  If not given, assumes the default `job_id`
  template.  `job_id` templates determine how work changes through your
  pipeline
- *`valid_if_or`* - (optional) Criteria that `job_id`s are matched against.
  If a `job_id` for an app does not match the given
  `valid_if_or` criteria, then the job is immediately marked as "skipped"


Here is a minimum viable configuration for an app:

    {
        "app_name": {
          "bash_cmd": "echo 123"
        }
    }

As you can see, there's not much to it.

Here is an example of a simple App_Ai` --> `App_Bi` relationship.
Also notice that the `bash_cmd` performs string interpolation so
applications can receive dynamically determined command-line parameters.

    {
        "App1": {
            "bash_cmd": "echo {app_name} is Running App 1 with {job_id}"
        },
        "App2": {
            "bash_cmd": "echo Running App 2. job_id contains date={date}"
            "depends_on": {"app_name": ["App1"]}
        }
    }

A slightly more complex variant of a `App_Ai` --> `App_Bi` relationship.
Notice that the `job_id` of the child app has changed, meaning a `preprocess`
job identified by `{date}_{client_id}` would kick off a `modelBuild` job
identified by `{date}_{client_id}_purchaseRevenue`

    {
        "preprocess": {
            "job_id": "{date}_{client_id}"
        },
        "modelBuild": {
            "job_id": "{date}_{client_id}_{target}"
            "depends_on": {
                "app_name": ["preprocess"],
                "target": ["purchaseRevenue"]
            }
        }
    }

A dependency graph demonstrating how `App_A` queues up multiple `App_Bi`.  In
this example, the completion of a `preprocess` job identified by
`20140101_1234` enqueues two `modelBuild` jobs:
`20140101_1234_purchaseRevenue` and `20140101_1234_numberOfPageviews`

    {
        "preprocess": {
            "job_id": "{date}_{client_id}"
        },
        "modelBuild"": {
            "job_id": "{date}_{client_id}_{target}"
            "depends_on": {
                "app_name": ["preprocess"],
                "target": ["purchaseRevenue", "numberOfPageviews"]
            }
        }
    }

The below configuration demonstrates how multiple `App_Ai` reduce to
`App_Bj`.  In other words, the `modelBuild` of `client1_purchaseRevenue`
cannot run (or be queued to run) until `preprocess` has completed these
`job_ids`: `20140601_client1`, `20140501_client1`, and `20140401_client1`.
The same applies to job, `client1_numberOfPageviews`.  However, it does not
matter whether `client1_numberOfPageviews` or `client1_purchaseRevenue` runs
first.  Also, notice that these dates are hardcoded.  If you would like to
create dates that aren't hardcoded, you should refer to the section,
[Configuration: Defining Dependencies with Two-Way
Functions](README.md#configuration-defining-dependencies-with-a-two-way-functions).

    {
        "preprocess": {
            "job_id": "{date}_{client_id}"
        },
        "modelBuild": {
            "job_id": "{client_id}_{target}"
            "depends_on": {
                "app_name": ["preprocess"],
                "target": ["purchaseRevenue", "numberOfPageviews"],
                "date": [20140601, 20140501, 20140401]
            }
        }
    }

We also enable boolean logic in dependency structures.  An app can
depend on `dependency_group_1` OR another dependency group.  Within a
dependency group, you can also specify that the dependencies come from one
different set of `job_ids` AND another set.  The AND and OR logic can also be
combined in one example, and this can result in surprisingly complex
relationships.  In this example, there are several things happening.  Firstly,
note that in order for any of modelBuild's jobs to be queued to run, either
the dependencies in `dependency_group_1` OR those in `dependency_group_2` must
be met.  Looking more closely at `dependency_group_1`, we can see that it
defines a list of key-value objects ANDed together using a list.
`dependency_group_1` will not be satisfied unless all of the following is true:
the three listed dates for `preprocess` have completed AND the two dates for
`otherPreprocess`  have completed.  In summary, the value of
`dependency_group_1` is a list.  The use of a list specifies AND logic, while
the declaration of different dependency groups specifies OR logic.

    {
        "preprocess": {
            "job_id": "{date}_{client_id}"
        },
        "modelBuild": {
            "job_id": "{client_id}_{target}"
            "depends_on": {
                "dependency_group_1": [
                    {"app_name": ["preprocess"],
                     "target": ["purchaseRevenue", "purchaseQuantity"],
                     "date": [20140601, 20140501, 20140401]
                    },
                    {"app_name": ["otherPreprocess"],
                     "target": ["purchaseRevenue", "purchaseQuantity"],
                     "date": [20120101, 20130101]
                    }
                  ],
                "dependency_group_2": {
                    "app_name": ["preprocess"],
                    "target": ["numberOfPageviews"],
                    "date": [20140615]
                }
            }


There are many structures that `depends_on` can take, and some are better than
others.  We've given you enough building blocks to express almost any
deterministic batch processing pipeline.


Configuration: Defining Dependencies with a Two-Way Functions
==============

 TODO  This isn't implemented yet.  

Two way functions allow users of Stolos to define arbitrarily complex
dependency relationships between jobs.  The general idea of a two-way function
is to define how the job, `App_Ai`, can spawn one or more children, `App_Bj`.
Being "two-way", this function must be able to identify child and parent
jobs.

   depends_on:
       {_func: "python.import.path.to.package.module.func", app_name: ...}


Configuration: Job Types
==============

The `job_type` specifier in the config defines how your application code
should run.  For example, should your code be treated as a bash job (and
executed in its own shell), or should it be an Apache Spark (python) job
that receives elements of a stream or a textFile instance?  The
following table defines different `job_type` options available.  Each
`job_type` has its own set of configuration options, and these are
available at the commandline and (possibly) in the app configuration.

For most use-cases, we recommend "bash" job type.  However, if a plugin seems
particularly useful, remember that running the application without Stolos may
require some extra code on your part.

 - `job_type`="bash"
    - `bash_cmd`
 - `job_type`="pyspark"
    - `pymodule` - a python import path to python application code.  ie.
      `stolos.examples.tasks.test_task`,
    - `spark_conf` - a dict of Spark config keys and values
    - `env` - a dict of environment variables and values
    - `env_from_os` - a list if os environment variables that should exist on
      the Spark driver
    - `uris` - a list of Spark files and pyFiles

(Developer note) Different `job_type`s correspond to specific "plugins"
recognized by Stolos.  One can extend Stolos to support custom `job_type`s.
You may wish to do this if you determine that it is more convenient have
similar apps re-use the same start-up and tear-down logic.  Keep in mind that
plugins generally violate Stolos's rule that it is ignorant of your runtime
environment, network topology, infrastructure, etc.  As a best practice,
try to make your plugin completely isolated from the rest of Stolos's codebase.
Refer to the developer documentation for writing custom plugins.


Developer's Guide
===============

Submitting a Pull Request
---------------

We love that you're interested in contributing to this project!  Hopefully,
this section will help you make a successful pull request to the project.

If you'd like to make a change, you should:

1. Create an issue and form a plan with maintainers on the issue tracker
1. Fork this repo, clone it to you machine, make changes in your fork,
   and then submit a Pull Request.  Google "How to submit a pull request" or
   [follow this guide](https://help.github.com/articles/using-pull-requests).
  - Before submitting the PR, run tests to verify your code is clean:

    ./bin/test_stolos.sh  # run the tests

1. Get code reviewed and iterate until PR is closed


Creating a plugin
---------------

Plugins define how Stolos should execute an Application's jobs.  By default,
Stolos supports executing bash applications and Python Spark (pyspark)
applications.  In general, just using the bash plugin is fine for most
scenarios.  However, we expose a plugin api so that you may more tightly couple
your running application to Stolos's runtime environment.  It might make sense
to do this if you want your application to share the same process as Stolos, or
perhaps you wish to standardize the way different applications are initialized.

If you wish to add another plugin to Stolos or use your own, please follow
these instructions.  If you wish to create a custom plugin, create a
python file that defines exactly two things:

  - It must define a `main(ns)` function.  `ns` is an `argparse.Namespace`
    instance.  This function should use the values defined by variables
    `ns.app_name` and `ns.job_id` (and whatever other ns variables) to execute
    some specific piece of code that exists somewhere.
  - It must define a `build_arg_parser` object that will populate the `ns`.
    Keep in mind that Stolos will also populate this ns and it will force you
    to avoid naming conflicts in argument options.

Boilerplate for a Stolos plugin is this:

```
from stolos.plugins import at, api

def main(ns):
    pass

build_arg_parser = at.build_arg_parser([...])
```

To use your custom plugin, in your application's configuration, set the
`job_type` to `python.import.path.to.your.module`.


Roadmap:
===============

Here are some improvements we are considering in the future:

- Support additional configuration backends
  - Some of these backends may support a web UI for creating, viewing and
    managing app config and dependencies
  - We currently support storing configuration in json file xor in Redis.
- Support different queue backends in addition to Zookeeper
  - (Stolos should use backends through a shared api of some sort)
- A web UI showing various things like:
  - Interactive dependency graph
  - Current job status
