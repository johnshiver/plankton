package main

import (
	"time"

	"github.com/johnshiver/plankton/task"
)

type HiTaskRunner struct {
	interval int `task_param`
	task     *task.Task
}

func (ht *HiTaskRunner) run() {
	for i := 0; i < 20; i++ {
		ht.task.Parent.ResultsChannel <- "HI"
		time.Sleep(1000 * time.Millisecond)
	}
}

func (ht *HiTask) GetTask() *task.Task {
	return ht.task
}

func newHiTaskRunner() *HiTask {
	task := task.NewTask(
		"HiTask",
		[]TaskRunner{},
	)
	return &HiTask{
		interval: 10,
		task:     task,
	}
}
