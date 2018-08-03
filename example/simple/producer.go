package main

import (
	"time"

	"github.com/johnshiver/plankton/task"
)

type Producer struct {
	*task.Task
	payload  string `task_param:""`
	interval int    `task_param:""`
}

func (p *Producer) Run() {
	for i := 0; i < p.interval; i++ {
		p.Parent.GetTask().ResultsChannel <- p.payload
		time.Sleep(1000 * time.Millisecond)
	}
}

func (p *Producer) GetTask() *task.Task {
	return p.Task
}

func NewProducer(payload string, interval int) *Producer {
	return &Producer{
		task.NewTask("Producer"),
		payload,
		interval,
	}
}
