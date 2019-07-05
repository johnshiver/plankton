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
)

const (
	WAITING  = "waiting"
	RUNNING  = "running"
	COMPLETE = "complete"
)

// Task ...
//
type Task struct {
	Name           string
	Children       []TaskRunner
	Parent         TaskRunner
	ResultsChannel chan string
	WorkerTokens   chan struct{}
	State          string
	Priority       int
	Params         []*TaskParam
	// start / end for execution
	start         time.Time
	end           time.Time
	DataProcessed int
	Logger        *log.Logger
	mux           sync.Mutex
	// start / end for date range covered by task
	RangeStart string
	RangeEnd   string
}

type TaskParam struct {
	Name string
	Data reflect.Value
}

// NewTask ...
//
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

// Start ...
func (ts *Task) Start() time.Time {
	ts.mux.Lock()
	defer ts.mux.Unlock()
	return ts.start
}

func (ts *Task) SetStart(s time.Time) {
	ts.mux.Lock()
	defer ts.mux.Unlock()
	ts.start = s

}

// End ...
func (ts *Task) End() time.Time {
	ts.mux.Lock()
	defer ts.mux.Unlock()
	return ts.end
}

func (ts *Task) SetEnd(e time.Time) {
	ts.mux.Lock()
	defer ts.mux.Unlock()
	ts.end = e

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

// SetTaskPriorities ...
//
// Runs DFS on rootTask of task DAG to set task priorities order of precedence
// Assumes rootTask is a valid dag.
func SetTaskPriorities(rootTask *Task) error {
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
	taskSet := make(map[string]struct{})

	taskQ := []*Task{}
	taskQ = append(taskQ, rootTask)
	for len(taskQ) > 0 {
		curr := taskQ[0]
		taskQ = taskQ[1:]

		_, ok := taskSet[curr.GetHash()]
		if ok {
			return false
		}
		taskSet[curr.GetHash()] = struct{}{}
		for _, child := range curr.Children {
			taskQ = append(taskQ, child.GetTask())
		}
	}
	return true
}
