package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/nu7hatch/gouuid"

	"github.com/johnshiver/plankton/task"
)

type TaskScheduler struct {
	root_runner task.TaskRunner
	uuid        *uuid.UUID
	record_run  bool

	// TODO: if i set params here, would be nice to automatically set those on all
	//       tasks in the dag

	// TODO: ensure that all tasks in DAG share the same params
}

func NewTaskScheduler(task_runner task.TaskRunner, record_run bool) (*TaskScheduler, error) {

	// set task params
	task_queue := []task.TaskRunner{}
	task_queue = append(task_queue, task_runner)
	for len(task_queue) > 0 {
		curr := task_queue[0]
		task_queue = task_queue[1:]
		task.SetTaskParams(curr)
		for _, child := range curr.GetTask().Children {
			task_queue = append(task_queue, child)
		}
	}

	dag_is_good := task.VerifyDAG(task_runner.GetTask())
	if !dag_is_good {
		return nil, fmt.Errorf("Root task runner isnt a valid Task DAG\n")
	}
	schedueler_uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to create uuid for scheduler")
	}
	return &TaskScheduler{root_runner: task_runner, uuid: schedueler_uuid, record_run: record_run}, nil
}
func (ts *TaskScheduler) PrintDAGState() {
	// clears the terminal. might not to add functionality to support other systems
	// https://stackoverflow.com/questions/22891644/how-can-i-clear-the-terminal-screen-in-go
	fmt.Print("\033[H\033[2J")

	fmt.Println(strings.Repeat("-", 45))
	fmt.Println("Current Task DAG Status")
	fmt.Println(strings.Repeat("-", 45))
	fmt.Println(ts.getDAGState())
	fmt.Println(strings.Repeat("-", 45))
}

// starts the root_runner
func (ts *TaskScheduler) Start() {

	scheduler_wg := &sync.WaitGroup{}
	scheduler_wg.Add(1)
	go task.RunTaskRunner(ts.root_runner, scheduler_wg)

	finished := make(chan struct{})
	go func() {
		scheduler_wg.Wait()
		finished <- struct{}{}
	}()

	// TODO: can i make a global 'results' channel that i can
	// hook into every task that is made, then access from
	// the schduler to get results

	ticker := time.NewTicker(time.Millisecond * 200)
	done := false
	for !done {
		select {
		case <-ticker.C:
			ts.PrintDAGState()
		case <-finished:
			fmt.Println("Finished!")
			done = true
		}
	}
	ts.PrintDAGState()
	if ts.record_run {
		ts.recordDAGRun()
	}
	return

}

// would be nice to have a way to reveal current state of task DAG
// each task can have a way to say if it is currently running / pending
func (ts *TaskScheduler) getDAGState() string {

	// TODO: add running time to each task
	//       and show how long a task took to complete once it is done

	dag_state_strings := []string{}

	root_task := ts.root_runner.GetTask()
	task_queue := []*task.Task{}
	task_queue = append(task_queue, root_task)
	for len(task_queue) > 0 {
		curr := task_queue[0]
		task_queue = task_queue[1:]
		var running_time time.Duration
		if curr.State == "complete" {
			running_time = curr.End.Sub(curr.Start)
		} else {
			running_time = time.Now().Sub(curr.Start)
		}

		curr_state := fmt.Sprintf("\t %s", curr.State)
		curr_run_time := fmt.Sprintf("\t %s", running_time)
		curr_data_processed := fmt.Sprintf("\t data processed so far: %d", curr.DataProcessed)
		dag_state_strings = append(dag_state_strings, curr.GetHash())
		dag_state_strings = append(dag_state_strings, curr_state)
		dag_state_strings = append(dag_state_strings, curr_run_time)
		if curr.DataProcessed > 0 {
			dag_state_strings = append(dag_state_strings, curr_data_processed)
		}

		for _, child := range curr.Children {
			task_queue = append(task_queue, child.GetTask())
		}

	}
	return strings.Join(dag_state_strings, "\n")
}

// re runs tasks associated with schduler uuid
func ReRunTask(root_task task.Task, scheduler_uuid string) {

}

type PlanktonRecord struct {
	gorm.Model
	TaskHash      string
	SchedulerUUID string
	ExecutionTime float64
}

func (ts *TaskScheduler) recordDAGRun() {
	// Some considerations: i have chosen for now to record the entire run after its completion
	// it may make sense for each task to individually record its state as it finishes
	// might make it clearner when errors occur / etc.  will revisit this once things are working a bit better

	// TODO: use a config for this stuff
	// TODO: consider using another database ORM or just pure sql, for now i want to get this working
	db, err := gorm.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=mytestdb password=mysecretpassword sslmode=disable")
	if err != nil {
		panic("failed to connect database")
	}
	defer db.Close()
	db.AutoMigrate(&PlanktonRecord{})

	root_task := ts.root_runner.GetTask()
	task_queue := []*task.Task{}
	task_queue = append(task_queue, root_task)
	for len(task_queue) > 0 {
		curr := task_queue[0]
		task_queue = task_queue[1:]
		execution_time := curr.End.Sub(curr.Start)

		new_plankton_record := PlanktonRecord{
			TaskHash:      curr.GetHash(),
			SchedulerUUID: ts.uuid.String(),
			ExecutionTime: execution_time.Seconds(),
		}
		db.Create(&new_plankton_record)
		for _, child := range curr.Children {
			task_queue = append(task_queue, child.GetTask())
		}
	}

}
