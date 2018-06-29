package task

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
)

type TaskRunner interface {
	Run()
	// requires TaskRuner to have an embedded Task
	GetTask() *Task
	SetTaskParams()
}

type Task struct {
	// TODO: does children need to be TaskRunner or can I get away with making everything
	//       a task
	Name           string
	Children       []TaskRunner
	Parent         *Task
	ResultsChannel chan string
	State          string
	Params         []string
}

func NewTask(name string, children []TaskRunner, parent *Task) *Task {
	return &Task{
		Name:           name,
		Children:       children,
		Parent:         parent,
		ResultsChannel: make(chan string),
		State:          "waiting",
		Params:         []string{},
	}
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()

}

func (ts *Task) GetTaskHash() string {
	param_string := strings.Join(ts.Params, "_")
	param_hash := hash(param_string)
	return fmt.Sprintf("{}_{}_{}", ts.Name, param_string, param_hash)
}

func (ts *Task) SetState(new_state string) (string, error) {
	valid_states := []string{
		"waiting",
		"running",
		"complete",
	}

	valid_state_param := false
	for _, state := range valid_states {
		if new_state == state {
			valid_state_param = true
			break
		}

	}

	if !valid_state_param {
		return "", fmt.Errorf("Invalid state on task {}", new_state)
	}

	ts.State = new_state
	return ts.State, nil

}

func RunTask(tsk_runner TaskRunner, wg *sync.WaitGroup) {
	tsk_runner.GetTask().SetState("running")
	fmt.Printf("Running Task: %s\n", tsk_runner.GetTask().Name)
	tsk_runner.Run()
	wg.Done()
	tsk_runner.GetTask().SetState("complete")

}
