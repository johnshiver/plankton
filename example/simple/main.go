package main

import (
	"log"

	"github.com/johnshiver/plankton/scheduler"
)

func main() {

	hi_task := NewProducer("hi", 25)
	lo_task := NewProducer("lo", 35)

	m1 := NewMultiplier(25)
	m2 := NewMultiplier(7)
	hiLoAgg := NewAggregator()

	m1.AddChildren(hi_task)
	m2.AddChildren(lo_task)
	hiLoAgg.AddChildren(m1, m2)

	my_scheduler, err := scheduler.NewTaskScheduler(hiLoAgg)
	if err != nil {
		log.Panic(err)
	}
	my_scheduler.Start()

}
