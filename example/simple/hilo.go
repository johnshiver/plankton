package main

import (
	"fmt"

	"github.com/johnshiver/plankton/task"
)

type HiLoAggregatorTask struct {
	task *task.Task
}

func NewHiLoTask() *HiLoAggregatorTask {
	task := task.NewTask(
		"HiLoAggregatorTask",
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
