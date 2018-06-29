package main

import (
	"github.com/johnshiver/plankton/scheduler"
)

func main() {
	// question: what is the mvp to get plankton working
	// create etl task DAG
	// check that it is actually a DAG
	// run children tasks in different go routines
	// and report their results to their parent task
	hiLoAgg := NewHiLoTask(nil)
	my_scheduler, _ := scheduler.NewTaskScheduler(hiLoAgg)
	my_scheduler.Start()

}
