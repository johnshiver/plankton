package main

import (
	"time"

	"github.com/johnshiver/plankton/task"
)

type HiTask struct {
	task *task.Task
}

func (ht *HiTask) Run() {
	for i := 0; i < 20; i++ {
		ht.task.Parent.ResultsChannel <- "HI"
		time.Sleep(1000 * time.Millisecond)
	}
}

func (ht *HiTask) GetTask() *task.Task {
	return ht.task
}

func (ht *HiTask) SetTaskParams() {
	return
}

func newHiTask(parent *task.Task) *HiTask {
	task := task.NewTask(
		"HiTask",
		[]task.TaskRunner{},
		parent,
	)
	return &HiTask{
		task: task,
	}
}
