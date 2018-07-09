package main

import (
	"time"

	"github.com/johnshiver/plankton/task"
)

type HiTaskRunner struct {
	interval int `task_param`
	task     *task.Task
}

func (ht *HiTaskRunner) Run() {
	for i := 0; i < 20; i++ {
		ht.GetTask().Parent.GetTask().ResultsChannel <- "HI"
		time.Sleep(1000 * time.Millisecond)
	}
}

func (ht *HiTaskRunner) GetTask() *task.Task {
	return ht.task
}

func NewHiTaskRunner() *HiTaskRunner {
	task := task.NewTask(
		"HiTask",
		[]task.TaskRunner{},
	)
	return &HiTaskRunner{
		interval: 10,
		task:     task,
	}
}
