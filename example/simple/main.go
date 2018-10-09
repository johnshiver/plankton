package main

import (
	"log"

	"github.com/johnshiver/plankton/borg"
	"github.com/johnshiver/plankton/scheduler"
	"github.com/johnshiver/plankton/terminal"
)

func main() {

	hi_task := NewProducer("hi", 5)
	lo_task := NewProducer("lo", 65)

	m1 := NewMultiplier(25)
	m2 := NewMultiplier(7)
	hiLoAgg := NewAggregator()

	m1.AddChildren(hi_task)
	m2.AddChildren(lo_task)
	hiLoAgg.AddChildren(m1, m2)

	SimpleScheduler, err := scheduler.NewTaskScheduler("Simple Scheduler", hiLoAgg, true)
	if err != nil {
		log.Panic(err)
	}

	borgScheduler, err := borg.NewBorgTaskScheduler(
		borg.AssimilatedScheduler{
			Scheduler:    SimpleScheduler,
			ScheduleSpec: "0 * * * * *"},
	)
	if err != nil {
		log.Panic(err)
	}

	go borgScheduler.Start()
	terminal.RunTerminal(borgScheduler)
}
