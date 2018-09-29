package scheduler

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/phf/go-queue/queue"

	"github.com/johnshiver/plankton/config"
	"github.com/johnshiver/plankton/task"
)

type TaskScheduler struct {
	root_runner task.TaskRunner
	uuid        *uuid.UUID
	record_run  bool
	nodes       []task.TaskRunner
}

func NewTaskScheduler(root_runner task.TaskRunner, record_run bool) (*TaskScheduler, error) {

	// set task params on runner DAG and create list of all task runners
	schedulerNodes := []task.TaskRunner{}
	task_queue := []task.TaskRunner{}
	task_queue = append(task_queue, root_runner)
	for len(task_queue) > 0 {
		curr := task_queue[0]
		schedulerNodes = append(schedulerNodes, curr)
		task_queue = task_queue[1:]
		task.CreateAndSetTaskParams(curr)
		for _, child := range curr.GetTask().Children {
			task_queue = append(task_queue, child)
		}
	}

	err := task.SetTaskPriorities(root_runner.GetTask())
	if err != nil {
		return nil, err
	}

	sort.Slice(schedulerNodes, func(i, j int) bool {
		return schedulerNodes[i].GetTask().Priority < schedulerNodes[j].GetTask().Priority
	})

	for _, node := range schedulerNodes {
		fmt.Println(node.GetTask().Name)
		fmt.Println(node.GetTask().Priority)
	}

	schedueler_uuid, err := uuid.NewV4()
	if err != nil {
		panic("Failed to create uuid for scheduler")
	}
	return &TaskScheduler{
		root_runner: root_runner,
		uuid:        schedueler_uuid,
		record_run:  record_run,
		nodes:       schedulerNodes}, nil
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
	c := config.GetConfig()
	workerTokens := []struct{}{}
	var numTokens int
	if c.ConcurrencyLimit > len(ts.nodes) {
		numTokens = len(ts.nodes)
	} else {
		numTokens = c.ConcurrencyLimit
	}

	for i := 0; i < numTokens; i++ {
		workerTokens = append(workerTokens, struct{}{})
	}

	tokenReturn := make(chan struct{})
	scheduler_wg := &sync.WaitGroup{}
	scheduler_wg.Add(1)
	go task.RunTaskRunner(ts.root_runner, scheduler_wg, tokenReturn)

	finished := make(chan struct{})
	go func() {
		scheduler_wg.Wait()
		finished <- struct{}{}
	}()

	taskPriority := 0
	for _, wToken := range workerTokens {
		ts.nodes[taskPriority].GetTask().WorkerTokens <- wToken
		taskPriority += 1
	}

	ticker := time.NewTicker(time.Millisecond * 200)
	done := false
	for !done {
		select {
		case <-ticker.C:
			ts.PrintDAGState()
		case <-finished:
			fmt.Println("Finished!")
			done = true
		case returnedToken := <-tokenReturn:
			if taskPriority < len(ts.nodes) {
				ts.nodes[taskPriority].GetTask().WorkerTokens <- returnedToken
				taskPriority += 1
			}
		}
	}
	ts.PrintDAGState()
	if ts.record_run {
		ts.recordDAGRun()
	}
}

func (ts *TaskScheduler) getDAGState() string {

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
		serialized_task_params := curr.GetSerializedParams()
		serialized_task_params = strings.TrimRight(serialized_task_params, "_")

		dag_state_strings = append(dag_state_strings, curr.Name+"=> "+serialized_task_params)
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

type TaskRunnerDepth struct {
	runner task.TaskRunner
	depth  int
}

// Figures out if DAGS are equal by performing breadth first search on each dag, sorting the
// slice of tasks by depth then hash, then comparing the slices for equality.
func AreTaskDagsEqual(task_dag1, task_dag2 task.TaskRunner) bool {

	task_dag1_runner_levels := []TaskRunnerDepth{}
	runner_q := queue.New()
	runner_q.PushBack(TaskRunnerDepth{task_dag1, 1})
	for runner_q.Len() > 0 {
		curr := runner_q.PopFront().(TaskRunnerDepth)
		task.CreateAndSetTaskParams(curr.runner)
		task_dag1_runner_levels = append(task_dag1_runner_levels, curr)
		for _, child := range curr.runner.GetTask().Children {
			runner_q.PushBack(TaskRunnerDepth{child, curr.depth + 1})
		}
	}

	// TODO: combine sorting methods to use one interface
	sort.Slice(task_dag1_runner_levels, func(i, j int) bool {
		if task_dag1_runner_levels[i].depth < task_dag1_runner_levels[j].depth {
			return true
		}
		if task_dag1_runner_levels[i].depth > task_dag1_runner_levels[j].depth {
			return false
		}
		return task_dag1_runner_levels[i].runner.GetTask().GetHash() < task_dag1_runner_levels[j].runner.GetTask().GetHash()
	})

	task_dag2_runner_levels := []TaskRunnerDepth{}
	runner_q = queue.New()
	runner_q.PushBack(TaskRunnerDepth{task_dag2, 1})
	for runner_q.Len() > 0 {
		curr := runner_q.PopFront().(TaskRunnerDepth)
		task.CreateAndSetTaskParams(curr.runner)
		task_dag2_runner_levels = append(task_dag2_runner_levels, curr)
		for _, child := range curr.runner.GetTask().Children {
			runner_q.PushBack(TaskRunnerDepth{child, curr.depth + 1})
		}
	}

	sort.Slice(task_dag2_runner_levels, func(i, j int) bool {
		if task_dag2_runner_levels[i].depth < task_dag2_runner_levels[j].depth {
			return true
		}
		if task_dag2_runner_levels[i].depth > task_dag2_runner_levels[j].depth {
			return false
		}
		return task_dag2_runner_levels[i].runner.GetTask().GetHash() < task_dag2_runner_levels[j].runner.GetTask().GetHash()
	})

	for i := 0; i < len(task_dag1_runner_levels); i++ {
		if i > len(task_dag2_runner_levels) {
			return false
		}

		r1 := task_dag1_runner_levels[i]
		r2 := task_dag2_runner_levels[i]

		if !(r1.runner.GetTask().GetHash() == r2.runner.GetTask().GetHash()) {
			return false
		}
	}

	return true
}

type TaskRunnerParentRecord struct {
	Runner       task.TaskRunner
	ParentRecord PlanktonRecord
}

/*
re runs previously scheduled task dag.  all tasks in a task dag runn share a scheduler uuid

which is the expected input.  root_dag
*/
func ReCreateStoredDag(root_dag task.TaskRunner, scheduler_uuid string) error {
	var records []PlanktonRecord
	c := config.GetConfig()
	c.DataBase.Where("scheduler_uuid = ?", scheduler_uuid).Find(&records)
	if len(records) < 1 {
		return fmt.Errorf("No records for task dag %s", scheduler_uuid)
	}

	runner_q := queue.New()
	runner_q.PushBack(TaskRunnerParentRecord{root_dag, PlanktonRecord{}})
	var curr TaskRunnerParentRecord
	var task_record_to_restore PlanktonRecord
	var found bool
	for runner_q.Len() > 0 {
		curr = runner_q.PopFront().(TaskRunnerParentRecord)

		// incase found was already initalized
		found = false
		for i, record := range records {
			if record.ParentHash == curr.ParentRecord.TaskHash {
				task_record_to_restore = record
				// remove record from records list...we wont use it again
				records = append(records[:i], records[i+1:]...)
				found = true
				break
			}
		}

		// NOTE: it might be good to allow for re-creations that have extra tasks, i.e.
		//       you add a new task and want to re-run this dag
		if !found {
			return fmt.Errorf("couldnt find a plankton record to restore to dag, check that your dag is correct")
		}

		task.CreateAndSetTaskParamsFromHash(curr.Runner, task_record_to_restore.TaskParams)
		curr.Runner.GetTask().Name = task_record_to_restore.TaskName
		for _, child := range curr.Runner.GetTask().Children {
			// TODO: do we need to set parent here? will be set when it's run by scheduler
			child.GetTask().Parent = curr.Runner
			runner_q.PushBack(TaskRunnerParentRecord{child, task_record_to_restore})
		}

	}

	return nil

}

// TODO: replace this with beego or standard library
type PlanktonRecord struct {
	gorm.Model
	TaskName      string
	TaskParams    string // should be nullable
	TaskHash      string
	ParentHash    string // should be nullable
	ChildHashes   string // should be nullable
	SchedulerUUID string
	ExecutionTime float64
}

func (ts *TaskScheduler) recordDAGRun() {
	/*
		Some considerations: i have chosen for now to record the entire run after its completion
		it may make sense for each task to individually record its state as it finishes
		might make it cleaner when errors occur / etc.  will revisit this once things are working a bit better

		TODO: consider using another database ORM or just pure sql, for now i want to get this working
	*/

	c := config.GetConfig()
	c.DataBase.AutoMigrate(PlanktonRecord{})

	root_task := ts.root_runner.GetTask()
	task_queue := []*task.Task{}
	task_queue = append(task_queue, root_task)
	for len(task_queue) > 0 {
		curr := task_queue[0]
		task_queue = task_queue[1:]
		execution_time := curr.End.Sub(curr.Start)

		var parent_hash string
		if curr.Parent != nil {
			parent_hash = curr.Parent.GetTask().GetHash()
		}

		var child_hash string
		if len(curr.Children) > 0 {
			child_hashes := []string{}
			for _, child := range curr.Children {
				child_hashes = append(child_hashes, child.GetTask().GetHash())
			}
			child_hash = strings.Join(child_hashes, ",")
		}
		new_plankton_record := PlanktonRecord{
			TaskName:      curr.Name,
			TaskParams:    curr.GetSerializedParams(),
			TaskHash:      curr.GetHash(),
			ParentHash:    parent_hash,
			ChildHashes:   child_hash,
			SchedulerUUID: ts.uuid.String(),
			ExecutionTime: execution_time.Seconds(),
		}
		c.DataBase.Create(&new_plankton_record)
		for _, child := range curr.Children {
			task_queue = append(task_queue, child.GetTask())
		}
	}

}
