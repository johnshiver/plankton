package main

import (
	"time"

	"github.com/johnshiver/plankton/task"
)

type Producer struct {
	*task.Task
	Payload  string `task_param:""`
	Interval int    `task_param:""`
}

func (p *Producer) Run() {
	for i := 0; i < p.Interval; i++ {
		p.Parent.GetTask().ResultsChannel <- p.Payload
		p.DataProcessed += 1
		time.Sleep(1000 * time.Millisecond)
	}
}

func (p *Producer) GetTask() *task.Task {
	return p.Task
}

func NewProducer(Payload string, Interval int) *Producer {
	return &Producer{
		task.NewTask("Producer"),
		Payload,
		Interval,
	}
}
