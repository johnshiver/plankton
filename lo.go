package main

import (
	"time"

	"github.com/johnshiver/plankton/task"
)

type LowTaskRunner struct {
	payload string `task_param`
	task    *task.Task
}

func (lt *LowTaskRunner) run() {
	for i := 0; i < 2; i++ {
		lt.task.Parent.ResultsChannel <- lt.payload
		time.Sleep(200 * time.Millisecond)
	}
}

func (lt *LowTaskRunner) GetTask() *task.Task {
	return lt.task
}

func NewLowTaskRunner() *LowTaskRunner {
	task := task.NewTask(
		"LoTask",
		[]task.TaskRunner{},
	)
	return &LoTaskRunner{
		payload: "LO",
		task:    task,
	}
}
