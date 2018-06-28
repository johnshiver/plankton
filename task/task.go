package task

import "sync"

type TaskRunner interface {
	Run()
	// requires TaskRuner to have an embedded Task
	GetTask() *Task
}

type Task struct {
	name            string
	children        []TaskRunner
	parent          *Task
	results_channel chan string
	state           string
}

func NewTask(name string, children []TaskRunner, parent *Task) *Task {
	return &Task{
		name:            name,
		children:        children,
		parent:          parent,
		results_channel: make(chan string),
		state:           "waiting",
	}
}

func (ts *Task) GetState() string {
	return ts.state

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
		return "", error
	}

	ts.state = new_state
	return ts.state, nil

}

func RunTask(tsk_runner TaskRunner, wg *sync.WaitGroup) {
	tsk_runner.GetTask().SetState("running")
	tsk_runner.Run()
	wg.Done()
	tsk_runner.GetTask().SetState("complete")

}
