package main

import (
	"fmt"
	"log"

	"github.com/johnshiver/plankton/borg"
	"github.com/johnshiver/plankton/scheduler"
	"github.com/johnshiver/plankton/terminal"
)

func createAgg(n, m int) *Aggregator {
	hi_task := NewProducer("hi", 15)
	lo_task := NewProducer("lo", 25)

	m1 := NewMultiplier(n)
	m2 := NewMultiplier(m)
	hiLoAgg := NewAggregator()

	m1.AddChildren(hi_task)
	m2.AddChildren(lo_task)
	hiLoAgg.AddChildren(m1, m2)
	return hiLoAgg
}

func runSchedulers() {
	agg1 := createAgg(10, 20)
	agg2 := createAgg(25, 30)
	agg3 := createAgg(10, 20)
	agg4 := createAgg(1, 2)
	agg4.AddChildren(NewProducer("pika123", 15))

	SimpleScheduler, err := scheduler.NewTaskScheduler("Simple Scheduler", "0 * * * * *", agg1, true)
	if err != nil {
		log.Panic(err)
	}
	SimpleScheduler2, err := scheduler.NewTaskScheduler("Simple Scheduler2", "10 * * * * *", agg2, true)
	if err != nil {
		log.Panic(err)
	}
	SimpleScheduler3, err := scheduler.NewTaskScheduler("Simple Scheduler3", "30 * * * * * ", agg3, true)
	if err != nil {
		log.Panic(err)
	}

	SimpleScheduler4, err := scheduler.NewTaskScheduler("Simple Scheduler4", "", agg4, true)
	if err != nil {
		log.Panic(err)
	}

	borgScheduler, err := borg.NewBorgTaskScheduler(
		SimpleScheduler, SimpleScheduler2, SimpleScheduler3, SimpleScheduler4,
	)
	if err != nil {
		log.Panic(err)
	}

	go borgScheduler.Start()
	terminal.RunTerminal(borgScheduler)
}

func scratch() {
	agg1 := createAgg(10, 20)
	SimpleScheduler, err := scheduler.NewTaskScheduler("Simple Scheduler", "0 * * * * *", agg1, true)
	if err != nil {
		log.Panic(err)
	}
	res := SimpleScheduler.LastRecords()
	for _, r := range res {
		fmt.Println(r)
	}
}

func main() {
	runSchedulers()

}
