package main

import (
	"strings"

	"github.com/johnshiver/plankton/task"
)

type MultiplierTaskRunner struct {
	multiplier int `task_param:""`
	task       *task.Task
}

func (mt *MultiplierTaskRunner) Run() {
	for data := range mt.GetTask().ResultsChannel {
		new_data := strings.Repeat(data, mt.multiplier)
		mt.GetTask().Parent.GetTask().ResultsChannel <- new_data
	}
}

func (mt *MultiplierTaskRunner) GetTask() *task.Task {
	return mt.task
}

func NewMultiTaskRunner(multiplier int) *MultiplierTaskRunner {
	task := task.NewTask("Mutliplier")
	return &MultiplierTaskRunner{
		multiplier: multiplier,
		task:       task,
	}
}
