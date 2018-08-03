package main

import (
	"fmt"

	"github.com/johnshiver/plankton/task"
)

type Aggregator struct {
	*task.Task
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		task.NewTask("Aggregator"),
	}
}

func (a *Aggregator) GetTask() *task.Task {
	return a.Task
}

func (a *Aggregator) Run() {

	for result := range a.ResultsChannel {
		fmt.Println(result)
	}

}
