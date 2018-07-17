package main

import (
	"log"

	"github.com/johnshiver/plankton/scheduler"
)

func main() {
	hi_task := NewHiTaskRunner()
	lo_task := NewLowTaskRunner()
	m1 := NewMultiTaskRunner(25)
	m2 := NewMultiTaskRunner(7)
	hiLoAgg := NewHiLoTask()

	m1.GetTask().AddChildren(hi_task)
	m2.GetTask().AddChildren(lo_task)
	hiLoAgg.GetTask().AddChildren(m1, m2)

	my_scheduler, err := scheduler.NewTaskScheduler(hiLoAgg)
	if err != nil {
		log.Panic(err)
	}
	my_scheduler.Start()

}
