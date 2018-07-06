package main

import (
	"fmt"

	"github.com/johnshiver/plankton/task"
)

type HiLoAggregatorTask struct {
	task *task.Task
}

func NewHiLoTask(parent *task.Task) *HiLoAggregatorTask {
	hiTaskRunner := newHiTaskRunner()
	loTaskRunner := newLowTaskRunner()
	task := task.NewTask(
		"HiLoAggregatorTask",
		[]TaskRunner{
			hiTaskRunner,
			lowTaskRunner,
		},
	)
	return &HiLoAggregatorTask{
		task: task,
	}

}

func (hl *HiLoAggregatorTask) GetTask() *task.Task {
	return hl.task
}

func (hl *HiLoAggregatorTask) run() {

	for result := range hl.task.ResultsChannel {
		fmt.Println(result)
	}

}
