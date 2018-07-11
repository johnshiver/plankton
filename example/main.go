package main

import (
	"log"

	"github.com/johnshiver/plankton/scheduler"
)

func main() {
	// question: what is the mvp to get plankton working
	// create etl task DAG
	// check that it is actually a DAG
	// run children tasks in different go routines
	// and report their results to their parent task
	hi_task := NewHiTaskRunner()
	lo_task := NewLowTaskRunner()
	m1 := NewMultiTaskRunner(25)
	m2 := NewMultiTaskRunner(30)
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
