package main

import (
	"fmt"
	"sync"
	"time"
)

// Im not entirely sure how polymorphism works in Go
// I want this interface to be implemented by all Task structs
// but I cant name it 'Task'
type TaskRunner interface {
	run(wg *sync.WaitGroup)
}

type Task struct {
	name            string
	children        []TaskRunner
	parent          *Task
	results_channel chan string
}

func NewTask(name string, children []TaskRunner, parent *Task) *Task {
	return &Task{
		name:            name,
		children:        children,
		parent:          parent,
		results_channel: make(chan string),
	}
}

type HiTask struct {
	task *Task
}

func (ht *HiTask) run(wg *sync.WaitGroup) {
	for i := 0; i < 20; i++ {
		ht.task.parent.results_channel <- "HI"
		time.Sleep(100 * time.Millisecond)
	}
	wg.Done()
}

func newHiTask(parent *Task) *HiTask {
	task := NewTask(
		"HiTask",
		[]TaskRunner{},
		parent,
	)
	return &HiTask{
		task: task,
	}
}

type LoTask struct {
	task *Task
}

func (lt *LoTask) run(wg *sync.WaitGroup) {
	for i := 0; i < 20; i++ {
		lt.task.parent.results_channel <- "LO"
		time.Sleep(100 * time.Millisecond)
	}
	wg.Done()
}
func newLowTask(parent *Task) *LoTask {
	task := NewTask(
		"LoTask",
		[]TaskRunner{},
		parent,
	)
	return &LoTask{
		task: task,
	}
}

type HiLoAggregatorTask struct {
	task *Task
}

func newHiLoTask(parent *Task) *HiLoAggregatorTask {
	new_hlagg := new(HiLoAggregatorTask)
	children := []TaskRunner{}
	task := NewTask(
		"HiLoAggregatorTask",
		children,
		nil,
	)
	new_hlagg.task = task
	hiTask := newHiTask(new_hlagg.task)
	loTask := newLowTask(new_hlagg.task)
	children = append(children, hiTask)
	children = append(children, loTask)
	task.children = children
	return new_hlagg

}

func (hl *HiLoAggregatorTask) run(wg *sync.WaitGroup) {

	local_wg := &sync.WaitGroup{}
	for _, runner := range hl.task.children {
		fmt.Println("Running Child Task")
		local_wg.Add(1)
		go runner.run(local_wg)
	}

	go func() {
		local_wg.Wait()
		close(hl.task.results_channel)

	}()

	for result := range hl.task.results_channel {
		fmt.Println(result)
	}
	wg.Done()
}

func main() {
	// question: what is the mvp to get plankton working
	// create etl task DAG
	// check that it is actually a DAG
	// run children tasks in different go routines
	// and report their results to their parent task
	hiLoAgg := newHiLoTask(nil)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Wait()
	}()
	hiLoAgg.run(wg)
	fmt.Println("Agg task done!")

}
