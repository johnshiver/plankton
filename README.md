# Plankton - a simple ETL framework for simple, fast ETLs

Plankton is a set of tools you can use to create ETL dags, that is, a graph of tasks
that need to be completed, each having dependencies such that it creates a
Directed, Acyclic Graph. Plankton was inspired by Luigi, which Ive used a lot over the past
six months to write ETLs at work.

Plankton works by defining TaskRunners, tasks that need to be run, which can have a single parent
(also a TaskRunner).

A plankton scheduler initalizes with a DAG root and runs the tasks in the DAG concurrently,
each running in a separate goroutine.

Data can be passed from a child task to its parent via the Task `ResultsChannel`, a string channel.
(perhaps that will change if we get generics in 2.0, or if I think up a more clever
mechanism)

Simple examples are provided in the example directory, one for defining a task that simply passes
strings from child to parent, and another example that reads from multiple csv files and inserts
the data into corresponding postgres tables.


## Storing DAG metadata + Task Parameters

Plankton can be configured to store metadata about your DAG runs, which can be used just for record
keeping, or to re-create previous runs, either with new code or different parameters.

Plankton TaskRunners can optinonally define fields marked as "task_param"s. A task_param is a
field whose value will be recorded in your DAG runs meta data, and whose value will be restored
if a re-run is executed.

Plankton TaskRunners come with two task_params out of the box, since and to, since it is a very
common pattern for each DAG run to stretch a certain length of time.

**Currently, Im working on automated DAG scheduling, think cron, that will automatically determine
  your since and to values.**


## What needs to be done

There is still a lot of work to be done to make Plankton feature complete.

- allow schedulers to run on a schedule : P think a cron type input
       scheduler := NewScheduler(task_dag, "* * * * *", record_results=True)
- The tools needed to re-create the DAG exist in the metadata, BUT an outstanding issue is whether
  or not the task GetHash algorithm doesnt have collisions.  If there are collisions such that
  two tasks with different parameters have the same hash, then the DAG cannot be effectively
  recreated.
- create API server for scheduler, so it can create dag runs.  can be used by eventual whale / borg
  package that schedules the tasks
