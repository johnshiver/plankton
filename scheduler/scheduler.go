package scheduler

import (
	"context"
	"errors"
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

// TaskScheduler ...
// The object responsible for taking the root node of a task DAG and running it via the Start() method.
type TaskScheduler struct {
	Name      string
	RootTask  *task.Task
	Logger    *log.Logger
	CronSpec  string
	status    string
	uuid      *uuid.UUID
	recordRun bool
	fullDag   []*task.Task
	mux       sync.Mutex
}

// GetTaskSchedulerLogFilePath ...
// Gven a TaskScheduler name returns log file path based on settings
func GetTaskSchedulerLogFilePath(schedulerName string) string {
	logFilePrefix := strings.ToLower(schedulerName)
	logFileName := fmt.Sprintf("%s-scheduler.log", logFilePrefix)
	c := config.GetConfig()
	loggingFilePath := c.LoggingDirectory + logFileName
	return loggingFilePath
}

// NewTaskScheduler ...
// Returns new task scheduler
func NewTaskScheduler(schedulerName, cronSpec string, rootTask *task.Task, recordRun bool) (*TaskScheduler, error) {
	loggingFilePath := GetTaskSchedulerLogFilePath(schedulerName)

	logConfig := &lumberjack.Logger{
		Filename:   loggingFilePath,
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}
	schedulerLogger := log.New(logConfig, "scheduler", log.LstdFlags)

	ok, err := task.VerifyDAG(rootTask)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("root task is not a valid dag")
	}

	rootTask.SetParentOnChildren()
	// set task params on runner DAG and create list of all task runners
	var (
		schedulerNodes []*task.Task
		taskQ          []*task.Task
	)
	taskQ = append(taskQ, rootTask)
	for len(taskQ) > 0 {
		curr := taskQ[0]
		schedulerNodes = append(schedulerNodes, curr)
		taskQ = taskQ[1:]
		curr.Logger = log.New(logConfig, curr.Name+"-", log.LstdFlags)
		for _, child := range curr.Children {
			taskQ = append(taskQ, child)
		}
	}

	err = task.SetTaskPriorities(rootTask)
	if err != nil {
		return nil, err
	}
	sort.Slice(schedulerNodes, func(i, j int) bool {
		return schedulerNodes[i].Priority < schedulerNodes[j].Priority
	})

	schedulerUUID, err := uuid.NewV4()
	if err != nil {
		panic("Failed to create uuid for scheduler")
	}
	return &TaskScheduler{
		Name:      schedulerName,
		CronSpec:  cronSpec,
		RootTask:  rootTask,
		status:    WAITING,
		uuid:      schedulerUUID,
		recordRun: recordRun,
		fullDag:   schedulerNodes,
		Logger:    schedulerLogger}, nil
}

// LastRun ...
// Returns date string of latest run
func (ts *TaskScheduler) LastRun() string {
	var records []PlanktonRecord
	c := config.GetConfig()
	c.DataBase.Order("started_at desc").Where("scheduler_name = ?", ts.Name).Find(&records)
	if len(records) < 1 {
		return "No Runs"
	}
	lastRecord := records[0]
	return lastRecord.EndedAt.Format(time.RFC822Z)
}

// Status ...
// Used to expose TaskScheduler.status in a thread safe way
func (ts *TaskScheduler) Status() string {
	ts.mux.Lock()
	defer ts.mux.Unlock()
	return ts.status
}

// SetStatus ...
// Used to set TaskScheduler.status in a thread safe way
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
	_ = ts.SetStatus(RUNNING)
	defer func() {
		_ = ts.SetStatus(WAITING)
	}()

	ts.prepareRootDagForRun()
	ts.setUUID()

	// set worker tokens, this is how we limit concurrency of tasks
	c := config.GetConfig()
	var workerTokens []struct{}
	var numTokens int
	if c.ConcurrencyLimit > len(ts.fullDag) {
		numTokens = len(ts.fullDag)
	} else {
		numTokens = c.ConcurrencyLimit
	}
	for i := 0; i < numTokens; i++ {
		workerTokens = append(workerTokens, struct{}{})
	}

	// each task returns its token upon completion
	tokenReturn := make(chan struct{})
	errorChan := make(chan error)
	schedulerWG := &sync.WaitGroup{}
	schedulerWG.Add(1)
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go task.RunTask(rootCtx, ts.RootTask, schedulerWG, tokenReturn, errorChan)

	finished := make(chan struct{})
	go func() {
		schedulerWG.Wait()
		finished <- struct{}{}
	}()

	taskPriority := 0
	for _, wToken := range workerTokens {
		ts.fullDag[taskPriority].WorkerTokens <- wToken
		taskPriority++
	}

	done := false
	for !done {
		select {
		case <-rootCtx.Done():
			ts.Logger.Print("scheduler context is done")
			done = true
		case <-finished:
			ts.Logger.Print("scheduler finished")
			done = true
		case returnedToken := <-tokenReturn:
			if taskPriority < len(ts.fullDag) {
				ts.fullDag[taskPriority].WorkerTokens <- returnedToken
				taskPriority++
			}
		}
	}
	if ts.recordRun {
		ts.recordDAGRun()
	}
	return
}

func (ts *TaskScheduler) setUUID() {
	schedulerUUID, err := uuid.NewV4()
	if err != nil {
		ts.Logger.Panic("Failed to create uuid for scheduler")
	}
	ts.uuid = schedulerUUID
}

// prepareRootDagForRun ...
// Ensures RootRunner has the correct state before starting a new run.  Since the same data structure is used between runs,
// it helps to clear state.
func (ts *TaskScheduler) prepareRootDagForRun() {
	ts.RootTask.SetParentOnChildren()
	task.ClearDAGState(ts.RootTask)
}

type TaskRunnerDepth struct {
	runner *task.Task
	depth  int
}

// AreTaskDagsEqual ...
// Figures out if DAGS are equal by performing breadth first search on each dag, sorting the
// slice of tasks by depth then hash, then comparing the slices for equality.
func AreTaskDagsEqual(taskDag1, taskDag2 *task.Task) bool {

	var taskDag1RunnerLevels []TaskRunnerDepth
	runnerQ := queue.New()
	runnerQ.PushBack(TaskRunnerDepth{taskDag1, 1})
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(TaskRunnerDepth)
		taskDag1RunnerLevels = append(taskDag1RunnerLevels, curr)
		for _, child := range curr.runner.Children {
			runnerQ.PushBack(TaskRunnerDepth{child, curr.depth + 1})
		}
	}

	// TODO: combine sorting methods to use one interface
	sort.Slice(taskDag1RunnerLevels, func(i, j int) bool {
		if taskDag1RunnerLevels[i].depth < taskDag1RunnerLevels[j].depth {
			return true
		}
		if taskDag1RunnerLevels[i].depth > taskDag1RunnerLevels[j].depth {
			return false
		}
		return taskDag1RunnerLevels[i].runner.GetTask().GetHash() < taskDag1RunnerLevels[j].runner.GetTask().GetHash()
	})

	taskDag2RunnerLevels := []TaskRunnerDepth{}
	runnerQ = queue.New()
	runnerQ.PushBack(TaskRunnerDepth{taskDag2, 1})
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(TaskRunnerDepth)
		task.CreateAndSetTaskParams(curr.runner)
		taskDag2RunnerLevels = append(taskDag2RunnerLevels, curr)
		for _, child := range curr.runner.GetTask().Children {
			runnerQ.PushBack(TaskRunnerDepth{child, curr.depth + 1})
		}
	}

	sort.Slice(taskDag2RunnerLevels, func(i, j int) bool {
		if taskDag2RunnerLevels[i].depth < taskDag2RunnerLevels[j].depth {
			return true
		}
		if taskDag2RunnerLevels[i].depth > taskDag2RunnerLevels[j].depth {
			return false
		}
		return taskDag2RunnerLevels[i].runner.GetTask().GetHash() < taskDag2RunnerLevels[j].runner.GetTask().GetHash()
	})

	for i := 0; i < len(taskDag1RunnerLevels); i++ {
		if i > len(taskDag2RunnerLevels) {
			return false
		}
		r1 := taskDag1RunnerLevels[i]
		r2 := taskDag2RunnerLevels[i]
		if !(r1.runner.GetTask().GetHash() == r2.runner.GetTask().GetHash()) {
			return false
		}
	}

	return true
}

// ReRun ...
//
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

type taskRunnerParentRecord struct {
	Runner       task.TaskRunner
	ParentRecord PlanktonRecord
}

// ReCreateStoredDag ...
// re creates previously scheduled task dag.  all tasks in a task dag runn share a scheduler uuid
// which is the expected
func ReCreateStoredDag(RootDAG task.TaskRunner, schedulerUUID string) error {
	var records []PlanktonRecord
	c := config.GetConfig()
	c.DataBase.Where("scheduler_uuid = ?", schedulerUUID).Find(&records)
	if len(records) < 1 {
		return fmt.Errorf("No records for task dag %s", schedulerUUID)
	}

	runnerQ := queue.New()
	runnerQ.PushBack(taskRunnerParentRecord{RootDAG, PlanktonRecord{}})
	var curr taskRunnerParentRecord
	var taskRecordToRestore PlanktonRecord
	var found bool
	for runnerQ.Len() > 0 {
		curr = runnerQ.PopFront().(taskRunnerParentRecord)

		// incase found was already initialized
		found = false
		for i, record := range records {
			if record.ParentHash == curr.ParentRecord.TaskHash {
				taskRecordToRestore = record
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

		task.CreateAndSetTaskParamsFromHash(curr.Runner, taskRecordToRestore.TaskParams)
		curr.Runner.GetTask().Name = taskRecordToRestore.TaskName
		for _, child := range curr.Runner.GetTask().Children {
			// TODO: do we need to set parent here? will be set when it's run by scheduler
			child.GetTask().Parent = curr.Runner
			runnerQ.PushBack(taskRunnerParentRecord{child, taskRecordToRestore})
		}
	}
	return nil
}

// PlanktonRecord ...
// The gorm model used to save meta data for each run, enabled by passing
//
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
	Priority      int
	StartedAt     time.Time
	EndedAt       time.Time
	Version       string
}

// Result ...
// Object whose data encapsulates a single task dag run
type Result struct {
	SchedulerUUID string
	Start         string
	End           string
	Version       string
}

// LastRecords ...
//
// Returns list of all plankton meta data results
func (ts *TaskScheduler) LastRecords() []Result {
	c := config.GetConfig()
	results := []Result{}
	c.DataBase.Table("plankton_records").
		Select("scheduler_uuid, min(started_at) as start, max(ended_at) as end, version").
		Where("scheduler_name = ?", ts.Name).
		Group("scheduler_uuid, version").
		Order("ended_at desc").
		Scan(&results)
	return results
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

	taskRoot := ts.RootRunner.GetTask()
	taskQ := []*task.Task{}
	taskQ = append(taskQ, taskRoot)
	for len(taskQ) > 0 {
		curr := taskQ[0]
		taskQ = taskQ[1:]
		executionTime := curr.End().Sub(curr.Start())

		var parentHash string
		if curr.Parent != nil {
			parentHash = curr.Parent.GetTask().GetHash()
		}

		var childHash string
		if len(curr.Children) > 0 {
			childHashes := []string{}
			for _, child := range curr.Children {
				childHashes = append(childHashes, child.GetTask().GetHash())
			}
			childHash = strings.Join(childHashes, ",")
		}
		newPlanktonRecord := PlanktonRecord{
			TaskName:      curr.Name,
			TaskParams:    curr.GetSerializedParams(),
			TaskHash:      curr.Hash(),
			ParentHash:    parentHash,
			ChildHashes:   childHash,
			SchedulerUUID: ts.uuid.String(),
			SchedulerName: ts.Name,
			ExecutionTime: executionTime.Seconds(),
			StartedAt:     curr.Start(),
			EndedAt:       curr.End(),
			Version:       c.Version,
			Priority:      curr.Priority,
		}
		c.DataBase.Create(&newPlanktonRecord)
		for _, child := range curr.Children {
			taskQ = append(taskQ, child.GetTask())
		}
	}

}
