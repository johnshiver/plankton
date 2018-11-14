package main

import (
	"strings"

	"github.com/johnshiver/plankton/task"
)

type Multiplier struct {
	*task.Task
	N int `task_param:""`
}

func (mt *Multiplier) Run() {
	for data := range mt.ResultsChannel {
		new_data := strings.Repeat(data, mt.N)
		mt.Parent.GetTask().ResultsChannel <- new_data
	}
}

func (mt *Multiplier) GetTask() *task.Task {
	return mt.Task
}

func NewMultiplier(n int) *Multiplier {
	return &Multiplier{
		task.NewTask("Multiplier"),
		n,
	}
}
