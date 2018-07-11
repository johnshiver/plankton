package main

import (
	"time"

	"github.com/johnshiver/plankton/task"
)

type LowTaskRunner struct {
	payload  string `task_param:""`
	interval int    `task_param:""`
	task     *task.Task
}

func (lt *LowTaskRunner) Run() {
	for i := 0; i < lt.interval; i++ {
		lt.GetTask().Parent.GetTask().ResultsChannel <- lt.payload
		time.Sleep(200 * time.Millisecond)
	}
}

func (lt *LowTaskRunner) GetTask() *task.Task {
	return lt.task
}

func NewLowTaskRunner() *LowTaskRunner {
	task := task.NewTask(
		"LoTask",
	)
	return &LowTaskRunner{
		payload:  "LO",
		interval: 20,
		task:     task,
	}
}
