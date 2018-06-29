package main

import (
	"time"

	"github.com/johnshiver/plankton/task"
)

type LoTask struct {
	task *task.Task
}

func (lt *LoTask) Run() {
	for i := 0; i < 2; i++ {
		lt.task.Parent.ResultsChannel <- "LO"
		time.Sleep(200 * time.Millisecond)
	}
}

func (lt *LoTask) GetTask() *task.Task {
	return lt.task
}

func (ht *LoTask) SetTaskParams() {
	return
}

func newLowTask(parent *task.Task) *LoTask {
	task := task.NewTask(
		"LoTask",
		[]task.TaskRunner{},
		parent,
	)
	return &LoTask{
		task: task,
	}
}
