package main

import (
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

	results := []string{}
	for result := range a.ResultsChannel {
		results = append(results, result)
		a.DataProcessed += 1
	}

}
