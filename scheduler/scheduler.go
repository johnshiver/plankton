package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/johnshiver/plankton/task"
)

type TaskScheduler struct {
	root_runner task.TaskRunner
}

func NewTaskScheduler(task_runner task.TaskRunner) (*TaskScheduler, error) {
	dag_is_good := VerifyDAG(task_runner.GetTask())
	if !dag_is_good {
		return nil, fmt.Errorf("Root task runner isnt a valid Task DAG\n")
	}
	return &TaskScheduler{root_runner: task_runner}, nil
}

// starts the root_runner
func (ts *TaskScheduler) Start() {

	scheduler_wg := &sync.WaitGroup{}
	scheduler_wg.Add(1)
	go task.RunTask(ts.root_runner, scheduler_wg)

	finished := make(chan struct{})

	go func() {
		scheduler_wg.Wait()
		finished <- struct{}{}
	}()

	// TODO: can i make a global 'results' channel that i can
	// hook into every task that is made, then access from
	// the schduler to get results

	ticker := time.NewTicker(time.Second * 5)
	done := false
	for !done {
		select {
		case <-ticker.C:
			fmt.Println(strings.Repeat("-", 45))
			fmt.Println("Current DAG State")
			fmt.Println(strings.Repeat("-", 45))
			fmt.Println(ts.getDAGState())
			fmt.Println(strings.Repeat("-", 45))
		case <-finished:
			fmt.Println("Finished!")
			done = true
		}
	}
	fmt.Println(strings.Repeat("-", 45))
	fmt.Println("Current DAG State")
	fmt.Println(strings.Repeat("-", 45))
	fmt.Println(ts.getDAGState())
	fmt.Println(strings.Repeat("-", 45))
	return

}

// would be nice to have a way to reveal current state of task DAG
// each task can have a way to say if it is currently running / pending
func (ts *TaskScheduler) getDAGState() string {

	dag_state_strings := []string{}

	root_task := ts.root_runner.GetTask()
	task_queue := []*task.Task{}
	task_queue = append(task_queue, root_task)
	for len(task_queue) > 0 {
		curr := task_queue[0]
		task_queue = task_queue[1:]

		curr_state_string := fmt.Sprintf("Task %s: %s", curr.Name, curr.State)
		dag_state_strings = append(dag_state_strings, curr_state_string)

		for _, child := range curr.Children {
			task_queue = append(task_queue, child.GetTask())
		}

	}
	return strings.Join(dag_state_strings, "\n")
}

// TODO: do a better job detecting the D part

// TODO: we also need some way of creating a map of tasks
//       to check acyclical property
//       Going to implement task_has on Task
func VerifyDAG(root_task *task.Task) bool {

	task_set := make(map[string]struct{})

	task_queue := []*task.Task{}
	task_queue = append(task_queue, root_task)
	for len(task_queue) > 0 {
		curr := task_queue[0]
		task_queue = task_queue[1:]

		_, ok := task_set[curr.GetTaskHash()]
		if ok {
			return false
		} else {
			task_set[curr.GetTaskHash()] = struct{}{}
		}
		for _, child := range curr.Children {
			task_queue = append(task_queue, child.GetTask())
		}

	}

	return true

}
