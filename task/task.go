package task

import "sync"

type TaskRunner interface {
	Run(wg *sync.WaitGroup)

	// requires TaskRuner to have an embedded Task
	GetTask() *Task
	GetState() string
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
		state:           "not_running",
	}
}
