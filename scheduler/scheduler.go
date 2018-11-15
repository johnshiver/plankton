package scheduler

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/phf/go-queue/queue"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"

	"github.com/johnshiver/plankton/config"
	"github.com/johnshiver/plankton/task"
)

const (
	WAITING = "waiting"
	RUNNING = "running"
)

type TaskScheduler struct {
	Name       string
	RootRunner task.TaskRunner
	Logger     *log.Logger
	CronSpec   string
	status     string
	uuid       *uuid.UUID
	recordRun  bool
	nodes      []task.TaskRunner
	mux        sync.Mutex
}

func GetTaskSchedulerLogFilePath(schedulerName string) string {
	logFilePrefix := strings.ToLower(schedulerName)
	logFileName := fmt.Sprintf("%s-scheduler.log", logFilePrefix)
	c := config.GetConfig()
	loggingFilePath := c.LoggingDirectory + logFileName
	return loggingFilePath
}

func NewTaskScheduler(schedulerName, cronSpec string, RootRunner task.TaskRunner, recordRun bool) (*TaskScheduler, error) {
	loggingFilePath := GetTaskSchedulerLogFilePath(schedulerName)

	logConfig := &lumberjack.Logger{
		Filename:   loggingFilePath,
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}
	schedulerLogger := log.New(logConfig, "scheduler", log.LstdFlags)

	task.SetParents(RootRunner)
	// set task params on runner DAG and create list of all task runners
	schedulerNodes := []task.TaskRunner{}
	taskQ := []task.TaskRunner{}
	taskQ = append(taskQ, RootRunner)
	for len(taskQ) > 0 {
		curr := taskQ[0]
		schedulerNodes = append(schedulerNodes, curr)
		taskQ = taskQ[1:]
		task.CreateAndSetTaskParams(curr)
		curr.GetTask().Logger = log.New(logConfig, curr.GetTask().Name+"-", log.LstdFlags)
		for _, child := range curr.GetTask().Children {
			taskQ = append(taskQ, child)
		}
	}

	err := task.SetTaskPriorities(RootRunner.GetTask())
	if err != nil {
		return nil, err
	}
	sort.Slice(schedulerNodes, func(i, j int) bool {
		return schedulerNodes[i].GetTask().Priority < schedulerNodes[j].GetTask().Priority
	})

	schedulerUUID, err := uuid.NewV4()
	if err != nil {
		panic("Failed to create uuid for scheduler")
	}
	return &TaskScheduler{
		Name:       schedulerName,
		CronSpec:   cronSpec,
		RootRunner: RootRunner,
		status:     WAITING,
		uuid:       schedulerUUID,
		recordRun:  recordRun,
		nodes:      schedulerNodes,
		Logger:     schedulerLogger}, nil
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

func (ts *TaskScheduler) LastRun() string {
	var records []PlanktonRecord
	c := config.GetConfig()
	c.DataBase.Order("started_at desc").Where("scheduler_name = ?", ts.Name).Find(&records)
	if len(records) < 1 {
		return "No Runs"
	}
	last_record := records[0]
	return last_record.EndedAt.Format(time.RFC822Z)
}

func (ts *TaskScheduler) Status() string {
	ts.mux.Lock()
	defer ts.mux.Unlock()
	return ts.status
}

func (ts *TaskScheduler) SetStatus(newStatus string) error {
	ts.mux.Lock()
	defer ts.mux.Unlock()
	ts.status = newStatus
	return nil
}

// Start ...
//
// Entry point for starting the DAG beginning at the RootRunner.
// Each call to Start() does a number of things:
//     1) create new UUID for the scheduler
//     2) starts all TaskRunners, taking into account concurrency limit
//     3) records output if recordRun is set to true
func (ts *TaskScheduler) Start() {
	if ts.Status() == RUNNING {
		ts.Logger.Println("Scheduler is currently running, skipping run")
		return
	}

	// setup
	task.SetParents(ts.RootRunner)
	task.ClearDAGState(ts.RootRunner)
	task.ResetDAGResultChannels(ts.RootRunner)
	ts.SetStatus(RUNNING)
	defer ts.SetStatus(WAITING)

	schedulerUUID, err := uuid.NewV4()
	if err != nil {
		ts.Logger.Panic("Failed to create uuid for scheduler")
	}
	ts.uuid = schedulerUUID

	// set worker tokens, this is how we limit concurrency of tasks
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

	// each task returns its token upon completion
	tokenReturn := make(chan struct{})
	schedulerWG := &sync.WaitGroup{}
	schedulerWG.Add(1)
	go task.RunTaskRunner(ts.RootRunner, schedulerWG, tokenReturn)

	finished := make(chan struct{})
	go func() {
		schedulerWG.Wait()
		finished <- struct{}{}
	}()

	taskPriority := 0
	for _, wToken := range workerTokens {
		ts.nodes[taskPriority].GetTask().WorkerTokens <- wToken
		taskPriority += 1
	}

	done := false
	for !done {
		select {
		case <-finished:
			done = true
		case returnedToken := <-tokenReturn:
			if taskPriority < len(ts.nodes) {
				ts.nodes[taskPriority].GetTask().WorkerTokens <- returnedToken
				taskPriority += 1
			}
		}
	}
	if ts.recordRun {
		ts.recordDAGRun()
	}
	return
}

func (ts *TaskScheduler) getDAGState() string {

	dag_state_strings := []string{}
	rootTask := ts.RootRunner.GetTask()
	taskQ := []*task.Task{}
	taskQ = append(taskQ, rootTask)
	for len(taskQ) > 0 {
		curr := taskQ[0]
		taskQ = taskQ[1:]
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
			taskQ = append(taskQ, child.GetTask())
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
	runnerQ := queue.New()
	runnerQ.PushBack(TaskRunnerDepth{task_dag1, 1})
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(TaskRunnerDepth)
		task.CreateAndSetTaskParams(curr.runner)
		task_dag1_runner_levels = append(task_dag1_runner_levels, curr)
		for _, child := range curr.runner.GetTask().Children {
			runnerQ.PushBack(TaskRunnerDepth{child, curr.depth + 1})
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
	runnerQ = queue.New()
	runnerQ.PushBack(TaskRunnerDepth{task_dag2, 1})
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(TaskRunnerDepth)
		task.CreateAndSetTaskParams(curr.runner)
		task_dag2_runner_levels = append(task_dag2_runner_levels, curr)
		for _, child := range curr.runner.GetTask().Children {
			runnerQ.PushBack(TaskRunnerDepth{child, curr.depth + 1})
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

func (ts *TaskScheduler) ReRun(schedulerUUID string) error {
	// TODO: im not sure this is adequate enough. The goal here is to avoid
	//       mutating a task runner that is already running
	if ts.Status() == RUNNING {
		ts.Logger.Println("ReRun: Alreadying running!")
		return nil
	}
	origUUID := ts.uuid.String()
	ts.Logger.Printf("Orig UUID is %s\n", origUUID)
	task.SetParents(ts.RootRunner)
	ts.recordDAGRun()
	err := ReCreateStoredDag(ts.RootRunner, schedulerUUID)
	if err != nil {
		ts.Logger.Fatal(err)
		return err
	}
	ts.Logger.Println("Starting rerun!")
	ts.Start()
	err = ReCreateStoredDag(ts.RootRunner, origUUID)
	if err != nil {
		ts.Logger.Fatal(err)
		return err
	}
	return nil
}

/*
re creates previously scheduled task dag.  all tasks in a task dag runn share a scheduler uuid

which is the expected input.  root_dag
*/
func ReCreateStoredDag(RootDAG task.TaskRunner, scheduler_uuid string) error {
	var records []PlanktonRecord
	c := config.GetConfig()
	c.DataBase.Where("scheduler_uuid = ?", scheduler_uuid).Find(&records)
	if len(records) < 1 {
		return fmt.Errorf("No records for task dag %s", scheduler_uuid)
	}

	runnerQ := queue.New()
	runnerQ.PushBack(TaskRunnerParentRecord{RootDAG, PlanktonRecord{}})
	var curr TaskRunnerParentRecord
	var task_record_to_restore PlanktonRecord
	var found bool
	for runnerQ.Len() > 0 {
		curr = runnerQ.PopFront().(TaskRunnerParentRecord)

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
			runnerQ.PushBack(TaskRunnerParentRecord{child, task_record_to_restore})
		}

	}

	return nil

}

type PlanktonRecord struct {
	gorm.Model
	TaskName      string
	TaskParams    string
	TaskHash      string
	ParentHash    string
	ChildHashes   string
	SchedulerUUID string
	SchedulerName string
	ExecutionTime float64
	StartedAt     time.Time
	EndedAt       time.Time
}
type Result struct {
	SchedulerUUID string
	Start         string
	End           string
}

func (ts *TaskScheduler) LastRecords() []Result {
	c := config.GetConfig()
	results := []Result{}
	c.DataBase.Table("plankton_records").
		Select("scheduler_uuid, min(started_at) as start, max(ended_at) as end").
		Where("scheduler_name = ?", ts.Name).
		Group("scheduler_uuid").
		Order("ended_at desc").
		Scan(&results)
	return results
}

func init() {
	c := config.GetConfig()
	c.DataBase.AutoMigrate(PlanktonRecord{})
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

	root_task := ts.RootRunner.GetTask()
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
			SchedulerName: ts.Name,
			ExecutionTime: execution_time.Seconds(),
			StartedAt:     curr.Start,
			EndedAt:       curr.End,
		}
		c = config.GetConfig()
		c.DataBase.Create(&new_plankton_record)
		for _, child := range curr.Children {
			task_queue = append(task_queue, child.GetTask())
		}
	}

}
