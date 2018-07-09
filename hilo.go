package main

import (
	"fmt"

	"github.com/johnshiver/plankton/task"
)

type HiLoAggregatorTask struct {
	task *task.Task
}

func NewHiLoTask(parent *task.Task) *HiLoAggregatorTask {
	hiTaskRunner := NewHiTaskRunner()
	loTaskRunner := NewLowTaskRunner()
	task := task.NewTask(
		"HiLoAggregatorTask",
		[]task.TaskRunner{
			hiTaskRunner,
			loTaskRunner,
		},
	)
	return &HiLoAggregatorTask{
		task: task,
	}

}

func (hl *HiLoAggregatorTask) GetTask() *task.Task {
	return hl.task
}

func (hl *HiLoAggregatorTask) Run() {

	for result := range hl.task.ResultsChannel {
		fmt.Println(result)
	}

}
