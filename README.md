[![Build Status](https://travis-ci.org/johnshiver/plankton.svg?branch=master)](https://travis-ci.org/johnshiver/plankton)
[![codecov](https://codecov.io/gh/johnshiver/plankton/branch/master/graph/badge.svg)](https://codecov.io/gh/johnshiver/plankton)
[![Go Report Card](https://goreportcard.com/badge/github.com/johnshiver/plankton)](https://goreportcard.com/report/github.com/johnshiver/plankton)
# Plankton - a simple ETL framework for simple, fast ETLs

Plankton is a set of tools you can use to create ETL dags, that is, a graph of tasks
that need to be completed, each having dependencies such that it creates a
Directed, Acyclic Graph. Plankton was inspired by Luigi, which Ive used a lot over the past
six months to write ETLs at work.

Simple examples are provided in the example directory, one for defining a task that simply passes
strings from child to parent, and another example that reads from multiple csv files and inserts
the data into corresponding postgres tables.


Important Plankton Objects:

## Task / TaskRunner

The Task and TaskRunner together make up the base unit of abstraction in Plankton.

Tasks are a struct with all the elements necessary to track Task dependencies, pass and receive
data between Tasks, and many other features.

Some important attributes:

```Parent``` and ```Children```: Say you have a Task DAG Like this

      A
     / \
     B  C

Adding dependencies works through the AddChildren() method

```
taskA := Task{}
taskB := Task{}
taskC := Task{}

taskA.AddChildren(taskB, taskC)
```
ResultsChannel: How A Child Sends Data To its Parent.

ResultsChannel is always a ```chan string```, so often I pass json strings from child
tasks to parent tasks. This is useful if you are creating an ETL, TaskB and TaskC could be
data sources, each passing their results to a parent task through the TaskB.Parent.GetTask().ResultsChannel

Priority: The order in which the task is run by the TaskScheduler

By default the TaskScheduler determines the priority of the task for you by performing DFS
on the Root TaskRunner. To override this behavior, set Priority value on your task.

NOTE: Plankton will not verify that your manual ordering is correct, that is on you : )


### TaskRunner

TaskRunner is the base unit you will actually define in code, to hook into the Plankton framework,
and is just the abstraction necessary for Plankton to accept many different "kinds" of Tasks
that all share the same plumbing code.

TaskRunners must define a ```Run()``` method, the method run by the TaskScheduler when your task
is scheduled, and the ```GetTask()``` method, which returns a *Task.  This is my attempt of
polymorphism is Golang.  Perhaps there is a better way to define an interface that will always
have a Task, but this worked pretty well.


## TaskScheduler

TaskSchedulers are the next layer of abstraction above the Task + TaskRunner. TaskSchedulers
initialize with a TaskRunner that should be the root of a Task DAG (Directed Acyclic Graph).
Plankton will verify that your root TaskRunner is a valid DAG, and upon running Start()
will assign a priority automatically to each task based on its dependencies.

The TaskScheduler runs all of its tasks concurrently, though you can limit the number of tasks
that will run simultanously by defining the ConcurrencyLimit setting.  See Configuration for more.

## BorgTaskScheduler

## Terminal GUI

## Logging

## Configuration Options

Plankton configuration is set through defining settings in config.yaml. See example.config.yaml.

### Database
```
DatabaseType: "sqlite3"
DatabaseHost: "plankton.db"
```

These settings define where Plankton Metadata will be stored.  (If I forgot to mention already,
the Plankton TaskScheduler takes a parameter recordRun which will / wont store Plankton metadata)

### ConcurrencyLimit

Default
```
ConcurrencyLimit: 4
```
This value affects how many of your tasks will run in parallel.  Remember, task priority
is calculated automatically by default, take care to consider whether your DAG can run correctly.

### LoggingDirectory

Default
```
LoggingDirectory: $HOME/.plankton_logs/
```
The directory where your scheduler and borg logs will be stored.

### ResultChannelSize

Default
```
ResultChannelSize: 10000
```
Tasks can send results to their parent task via the parent's ResultChannel.  The ResultChannel
is a string channel with a default size of 10000.

### Version
Default
```
Version: "UNVERSIONED"
```
Version is saved to the plankton meta data for each time your scheduler completes a DAG run.
This can be useful for debugging, mapping data sets to the code that loaded / transformed it.
