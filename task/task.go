package task

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/johnshiver/plankton/config"
	"github.com/phf/go-queue/queue"
)

const (
	WAITING  = "waiting"
	RUNNING  = "running"
	COMPLETE = "complete"
)

type TaskRunner interface {
	// NOTE: Run() should always be called by RunTaskRunner

	// TODO: have Run() return an error, which could be given to scheduler
	Run()

	// requires TaskRuner to have an embedded Task
	GetTask() *Task
}

type Task struct {
	Name           string
	Children       []TaskRunner
	Parent         TaskRunner
	ResultsChannel chan string
	WorkerTokens   chan struct{}
	State          string
	Priority       int
	Params         []*TaskParam
	Start          time.Time
	End            time.Time
	DataProcessed  int
	Logger         *log.Logger
	mux            sync.Mutex
}

type TaskParam struct {
	Name string
	Data reflect.Value
}

func NewTask(name string) *Task {
	c := config.GetConfig()
	var (
		buf    bytes.Buffer
		logger = log.New(&buf, "logger: ", log.Lshortfile)
	)
	return &Task{
		Name:           name,
		Children:       []TaskRunner{},
		Parent:         nil,
		ResultsChannel: make(chan string, c.ResultChannelSize),
		WorkerTokens:   make(chan struct{}, c.ConcurrencyLimit),
		Priority:       -1,
		State:          WAITING,
		Params:         []*TaskParam{},
		DataProcessed:  0,
		Logger:         logger,
	}
}

func makeStringHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()

}

// GetHash ...
// Returns a hash representation of the task.  The elements that comprise the hash are
//
//  1. Task.Name
//  2. Serialized Task Params
//  3. All Child Serialized Params
//
//  These elements are joined together and hashed, which creates a fairly unique value.
//  This is used primarily to rebuild a TaskRunner from its metadata table or determine
//  whether two task DAGs are equal.
func (ts *Task) GetHash() string {
	stringsToHash := []string{}
	stringsToHash = append(stringsToHash, ts.Name)
	stringsToHash = append(stringsToHash, ts.GetSerializedParams())
	for _, child := range ts.Children {
		stringsToHash = append(stringsToHash, child.GetTask().GetSerializedParams())
	}
	finalHash := strings.Join(stringsToHash, "")
	return fmt.Sprintf("%v", (makeStringHash(finalHash)))
}

/*
   Given a Task, return a serialized string to represent it.
This is useful for storing the state of each Task that is run in the dag, and can
   be used to re-create a previously run task for DAG re-runs, backfills, or some other
   use case.

   Example:

    type TestTask struct {
	    *Task
	    N int    `task_param:""`
	    X string `task_param:""`
	    Z int
    }

    new_test_task := TestTask{
	5,
	"HI",
	0,
    }
    serialized_test_task := new_test_task.GetSerializedParams()
    fmt.Println(serialized_test_task)
    > N:INT:5_X:STR:HI

    This function uses a lot of reflection / is kind of tricky. Would be good to document
    exactly how this works because I constantly forget what these variables mean :P
*/
func (ts *Task) GetSerializedParams() string {

	paramStrings := []string{}
	for _, param := range ts.Params {
		var dataVal, dataType string

		// TODO: Should support all types in SetParam
		//       maybe there is a way to combine the logic

		// TODO: add support for some Date type, that should be enough to start
		//       working on running DAGS, storing their state, then re-running them
		//       with another command
		switch param.Data.Kind() {
		case reflect.Int:
			dataVal = fmt.Sprintf("%v", param.Data.Int())
			dataType = "INT"
		case reflect.String:
			dataVal = param.Data.String()
			dataType = "STR"
		default:
			// TODO: think about what to do in this case
			fmt.Printf("Param %s not included in the serialized task, its type is not currently supported.\n", param.Name)
		}

		paramSerializerElements := []string{
			param.Name,
			dataType,
			dataVal,
		}
		paramStrings = append(paramStrings, strings.Join(paramSerializerElements, ":"))
	}
	return strings.Join(paramStrings, "_")

}

func DeserializeTaskParams(serializedTaskParams string) ([]*TaskParam, error) {
	deserializedTaskParams := []*TaskParam{}
	if len(serializedTaskParams) < 1 {
		return deserializedTaskParams, nil
	}
	hashedParams := strings.Split(serializedTaskParams, "_")
	for _, param := range hashedParams {
		splitVals := strings.Split(param, ":")
		name, dType, dVal := splitVals[0], splitVals[1], splitVals[2]
		var finalVal reflect.Value

		switch dType {
		case "INT":
			finalInt, err := strconv.Atoi(dVal)
			if err != nil {
				return nil, fmt.Errorf("Error creating int from serialized Task Param")
			}
			finalVal = reflect.ValueOf(finalInt)
		case "STR":
			finalVal = reflect.ValueOf(dVal)
		default:
			spew.Println(finalVal)
			fmt.Println("Not supported yet")
			continue
		}

		newParam := &TaskParam{
			Name: name,
			Data: finalVal,
		}
		deserializedTaskParams = append(deserializedTaskParams, newParam)

	}
	return deserializedTaskParams, nil

}

// CreateAndSetTaskParams ...
// Uses reflection to inspect struct elements for 'task_param' tag
// and sets tr.Task.Params accordingly
func CreateAndSetTaskParams(tr TaskRunner) ([]*TaskParam, error) {

	const TASK_PARAM_TAG = "task_param"
	var taskParams []*TaskParam

	v := reflect.ValueOf(tr).Elem()
	for i := 0; i < v.NumField(); i++ {
		fieldInfo := v.Type().Field(i)
		tag := fieldInfo.Tag
		_, ok := tag.Lookup(TASK_PARAM_TAG)
		if ok {
			newParam := TaskParam{
				Name: fieldInfo.Name,
				Data: getFieldValue(tr, fieldInfo.Name),
			}
			taskParams = append(taskParams, &newParam)

		}
	}

	tr.GetTask().Params = taskParams
	return tr.GetTask().Params, nil
}

func getFieldValue(tr TaskRunner, fieldName string) reflect.Value {
	// TODO: check this works on different types other than int
	trReflect := reflect.ValueOf(tr)
	fieldVal := reflect.Indirect(trReflect).FieldByName(fieldName)
	return fieldVal
}

// CreateTaskRunnerFromParams ...
// Given Params and a TaskRunner, sets all TaskRunner fields marked as param
// Note: The struct satisfying the TaskRunner interface MUST be passed to this function
// as a reference.  See these articles for a more thorough explanation:
// https://stackoverflow.com/questions/6395076/using-reflect-how-do-you-set-the-value-of-a-struct-field
// http://speakmy.name/2014/09/14/modifying-interfaced-go-struct/
func CreateTaskRunnerFromParams(tr TaskRunner, params []*TaskParam) error {

	// TODO: change name of this Function.  It doesnt really create a task runner so much
	//       as fill in param values on an existing TaskRunner

	// TODO: make this var name consistent with the one used above
	stype := reflect.ValueOf(tr).Elem()

	paramNameValueMap := map[string]reflect.Value{}
	for _, param := range params {
		paramNameValueMap[param.Name] = param.Data
	}

	if stype.Kind() == reflect.Struct {
		for name, val := range paramNameValueMap {
			f := stype.FieldByName(name)
			if f.CanSet() {
				// TODO: support more kinds of fields
				switch f.Kind() {
				case reflect.Int:
					f.SetInt(val.Int())
				case reflect.String:
					f.SetString(val.String())
				default:
					// TODO: think about what to do in this case
					return fmt.Errorf("%s not supported as TaskParam yet", f.Kind())
				}

			} else {
				tr.GetTask().Logger.Printf("Cannot set %s %v\n", name, f)
			}
		}
	}

	return nil

}

// CreateAndSetTaskParamsFromHash ....
//
func CreateAndSetTaskParamsFromHash(tr TaskRunner, paramHash string) error {
	if tr.GetTask() == nil {
		return fmt.Errorf("tr %v didnt have task set", tr)
	}

	taskParams, err := DeserializeTaskParams(paramHash)
	if err != nil {
		return err
	}

	err = CreateTaskRunnerFromParams(tr, taskParams)
	if err != nil {
		return err
	}

	CreateAndSetTaskParams(tr)
	return nil

}

// AddChildren ...
//
func (ts *Task) AddChildren(children ...TaskRunner) []TaskRunner {
	ts.mux.Lock()
	defer ts.mux.Unlock()

	var tChildren []TaskRunner
	if len(ts.Children) < 1 {
		tChildren = []TaskRunner{}

	} else {
		tChildren = ts.Children
	}
	for _, child := range children {
		tChildren = append(tChildren, child)
	}

	ts.Children = tChildren
	return ts.Children
}

// SetState ...
//
func (ts *Task) SetState(newState string) (string, error) {
	ts.mux.Lock()
	defer ts.mux.Unlock()

	validStates := []string{
		WAITING, RUNNING, COMPLETE,
	}

	ValidStateParam := false
	for _, state := range validStates {
		if newState == state {
			ValidStateParam = true
			break
		}
	}

	if !ValidStateParam {
		return "", fmt.Errorf("Invalid state on task %s", newState)
	}

	ts.State = newState
	return ts.State, nil
}

func SetParents(tRunner TaskRunner) {
	for _, child := range tRunner.GetTask().Children {
		child.GetTask().Parent = tRunner
		SetParents(child)
	}
}

// RunTaskRunner ...
// Runs a TaskRunner, sets state and notifies waiting group when run is done
func RunTaskRunner(tRunner TaskRunner, wg *sync.WaitGroup, TokenReturn chan struct{}) {
	// TODO: add that failsafe i read in rob fig's cron project
	defer wg.Done()

	runnerChildren := tRunner.GetTask().Children
	if len(runnerChildren) > 0 {

		parentWG := &sync.WaitGroup{}
		for _, child := range runnerChildren {
			parentWG.Add(1)
			go RunTaskRunner(child, parentWG, TokenReturn)
		}

		go func() {
			parentWG.Wait()
			close(tRunner.GetTask().ResultsChannel)
		}()
	}

	done := false
	var token struct{}
	for !done {
		select {
		case token = <-tRunner.GetTask().WorkerTokens:
			tRunner.GetTask().Logger.Printf("Starting task")
			tRunner.GetTask().Start = time.Now()
			tRunner.GetTask().SetState(RUNNING)
			tRunner.Run()
			tRunner.GetTask().SetState(COMPLETE)
			tRunner.GetTask().End = time.Now()
			tRunner.GetTask().Logger.Printf("Task finished")
			TokenReturn <- token
			done = true
		}
	}
}

// SetTaskPriorities ...
//
func SetTaskPriorities(rootTask *Task) error {
	/*
	   Runs DFS on rootTask of task DAG to set task priorities order of precedence
	   Assumes rootTask is a valid dag.
	*/
	goodDag := verifyDAG(rootTask)
	if !goodDag {
		return fmt.Errorf("Root task runner isnt a valid Task DAG")
	}
	currPriority := 0
	var setTaskPriorities func(root *Task)
	setTaskPriorities = func(root *Task) {
		for _, child := range root.Children {
			setTaskPriorities(child.GetTask())
		}
		root.Priority = currPriority
		root.Logger.Printf("Priority set: %d\n", root.Priority)
		currPriority++
	}

	setTaskPriorities(rootTask)
	return nil
}

func verifyDAG(rootTask *Task) bool {

	// TODO: do a better job detecting the D part
	taskSet := make(map[string]struct{})

	taskQ := []*Task{}
	taskQ = append(taskQ, rootTask)
	for len(taskQ) > 0 {
		curr := taskQ[0]
		taskQ = taskQ[1:]

		_, ok := taskSet[curr.GetHash()]
		if ok {
			return false
		} else {
			taskSet[curr.GetHash()] = struct{}{}
		}
		for _, child := range curr.Children {
			taskQ = append(taskQ, child.GetTask())
		}

	}

	return true
}

// ResetDAGResultChannels ...
// Need to recreate result channels after each scheduler run, because they are close
// in RunTaskRunner
func ResetDAGResultChannels(RootRunner TaskRunner) {
	c := config.GetConfig()
	runnerQ := queue.New()
	runnerQ.PushBack(RootRunner)
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(TaskRunner)
		curr.GetTask().ResultsChannel = make(chan string, c.ResultChannelSize)
		for _, child := range curr.GetTask().Children {
			runnerQ.PushBack(child)
		}
	}
}

// ClearDAGState ...
//
func ClearDAGState(RootRunner TaskRunner) {
	runnerQ := queue.New()
	runnerQ.PushBack(RootRunner)
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(TaskRunner)
		curr.GetTask().DataProcessed = 0
		curr.GetTask().Start = time.Time{}
		curr.GetTask().End = time.Time{}

		for _, child := range curr.GetTask().Children {
			runnerQ.PushBack(child)
		}
	}
}
