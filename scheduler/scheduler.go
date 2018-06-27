package scheduler

import "sync"

type TaskScheduler struct {
	root_runner task.TaskRunner
	task_dag    []*task.Task
}

func NewTaskScheduler(task_runner task.TaskRunner) *TaskScheduler {

	return &TaskScheduler{root_runner: task_runner}

}

func (ts *TaskScheduler) verifyDAG() bool {

	return false

}

// starts the root_runner
func (ts *TaskScheduler) start() {
	good_dag := ts.verifyDAG()
	if good_dag != true {
		// TODO: raise some error
		return
	}
	scheduler_wg := &sync.WaitGroup{}
	scheduler_wg.Add(1)
	go func() {
		scheduler_wg.Wait()
	}()
	ts.root_runner.run(scheduler_wg)

}

// would be nice to have a way to reveal current state of task DAG
// each task can have a way to say if it is currently running / pending
func (ts *TaskScheduler) getDAGState() {

}

func (ts *TaskScheduler) CreateDAGfromRootTask() {

}
