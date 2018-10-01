package main

import (
	"log"

	"github.com/johnshiver/plankton/borg"
	"github.com/johnshiver/plankton/scheduler"
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

	simpleScheduler, err := scheduler.NewTaskScheduler(hiLoAgg, true)
	if err != nil {
		log.Panic(err)
	}

	borgScheduler, err := borg.NewBorgTaskScheduler(
		borg.AssimilatedScheduler{
			Name:         "Simple Scheduler",
			Scheduler:    simpleScheduler,
			ScheduleSpec: "0 * * * * *"},
	)
	if err != nil {
		log.Panic(err)
	}

	borgScheduler.Start()
}
