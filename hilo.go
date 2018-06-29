package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/johnshiver/plankton/task"
)

type HiLoAggregatorTask struct {
	task *task.Task
}

func NewHiLoTask(parent *task.Task) *HiLoAggregatorTask {
	new_hlagg := new(HiLoAggregatorTask)
	children := []task.TaskRunner{}
	task := task.NewTask(
		"HiLoAggregatorTask",
		children,
		nil,
	)
	new_hlagg.task = task
	hiTask := newHiTask(new_hlagg.task)
	loTask := newLowTask(new_hlagg.task)
	children = append(children, hiTask)
	children = append(children, loTask)
	task.Children = children
	return new_hlagg

}

func (hl *HiLoAggregatorTask) GetTask() *task.Task {
	return hl.task
}

func (hl *HiLoAggregatorTask) SetTaskParams() {
	return
}

func (hl *HiLoAggregatorTask) Run() {

	local_wg := &sync.WaitGroup{}
	for _, runner := range hl.task.Children {
		local_wg.Add(1)
		go task.RunTask(runner, local_wg)
	}

	go func() {
		local_wg.Wait()
		close(hl.task.ResultsChannel)

	}()

	for result := range hl.task.ResultsChannel {
		fmt.Println(result)
	}
	time.Sleep(5 * time.Second)

}
